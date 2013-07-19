package MapReduce;
use Moo;
use Storable qw(nfreeze thaw);
use Data::Dump::Streamer qw(Dump);
use Time::HiRes qw(time);
use Carp qw(croak);
use List::MoreUtils qw(all);
use Exporter qw(import);
use MapReduce::Mapper;
use MapReduce::Reducer;

our @EXPORT_OK = qw(pmap);

# Do not change these. Rather, to enable logging,
# change the $LOGGING value to one of these variables.
our $DEBUG = 2;
our $INFO  = 1;
our $NONE  = 0;

# Enable / disable logging.
our $LOGGING = $ENV{MAPREDUCE_LOGGING} // $NONE;

sub debug { shift->log('DEBUG', @_) if $LOGGING >= $DEBUG }
sub info  { shift->log('INFO',  @_) if $LOGGING >= $INFO  }

sub log {
    my ($class, $level, $format, @args) = @_;

    $format //= '';
    
    printf STDERR $level.': '.$format."\n", map { defined $_ ? $_ : 'undef' } @args;
}

has [ qw( name mapper ) ] => (
    is       => 'ro',
    required => 1,
);

has reducer => (
    is      => 'ro',
    default => sub { sub { $_[1] } },
);

has id => (
    is => 'lazy',
);

with 'MapReduce::Role::Redis';

sub _build_id {
    my ($self) = @_;
    
    my $id = 'mr-'.$self->name . '-' . int(time) . '-' . $$ . '-' . int(rand(2**31));
    
    MapReduce->debug( "ID is '%s'", $id );
    
    return $id;
}

sub BUILD {
    my ($self) = @_;
    
    my $mapper  = ref $self->mapper  ? Dump( $self->mapper  )->Declare(1)->Out() : $self->mapper;
    my $reducer = ref $self->reducer ? Dump( $self->reducer )->Declare(1)->Out() : $self->reducer;
    my $redis   = $self->redis;
    
    MapReduce->debug( "Mapper is '%s'",  $mapper );
    MapReduce->debug( "Reducer is '%s'", $reducer );
   
    $SIG{INT} = sub { exit 1 }
        if !$SIG{INT};
        
    $SIG{TERM} = sub { exit 1 }
        if !$SIG{TERM};
    
    $redis->hset( mapper  => ( $self->id => $mapper  ) );
    $redis->hset( reducer => ( $self->id => $reducer ) );
}

sub input {
    my $self = shift;
    
    croak 'Please surround your input operation with input_start() and input_done()'
        if !$self->redis->get($self->id.'-inputting');
    
    # Support a hash or hash ref for the input
    my $input = @_ == 1 ? shift : {@_};
    
    my $redis = $self->redis;
    
    $redis->lpush( $self->id.'-input', nfreeze($input) );
    
    $redis->incr( $self->id.'-input-count' );
    $redis->incr( $self->id.'-input-total' );
    
    MapReduce->debug( "Pushed input '%s' to %s-input.", $input->{key}, $self->id );
    
    return $self;
}

sub inputs {
    my ($self, $inputs) = @_;
    
    my $redis = $self->redis;
    
    $self->input_start;
    
    $redis->incrby( $self->id.'-input-total', scalar(@$inputs) );
    
    for my $input (@$inputs) {
        $redis->lpush( $self->id.'-input', nfreeze($input) );
        $redis->incr( $self->id.'-input-count' );
    }
    
    MapReduce->debug( "Pushed %d inputs %s-input.", scalar(@$inputs), $self->id );
    
    $self->input_done;
    
    return $self;
}

sub input_start {
    my ($self) = @_;
    
    $self->redis->set( $self->id.'-inputting', 1 );
}

sub input_done {
    my ($self) = @_;
    
    $self->redis->del( $self->id.'-inputting' );
}

sub done {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $id    = $self->id;
    
    $redis->multi;
    
    $redis->llen( $id.'-input'     );
    $redis->llen( $id.'-mapped'    );
    $redis->llen( $id.'-reduced'   );
    $redis->get(  $id.'-mapping'   );
    $redis->get(  $id.'-reducing'  );
    $redis->get(  $id.'-inputting' );
    
    my $values = $redis->exec;
    
    my $done = all { !$_ } @$values;
    
    MapReduce->debug( "Input queue:   %s", $values->[0] );
    MapReduce->debug( "Mapped queue:  %s", $values->[1] );
    MapReduce->debug( "Reduced queue: %s", $values->[2] );
    MapReduce->debug( "Mapping?:      %s", $values->[3] );
    MapReduce->debug( "Reducing?:     %s", $values->[4] );
    MapReduce->debug( "Inputting?:    %s", $values->[5] );
    
    return $done;
}

sub DEMOLISH {
    my ($self) = @_;
    
    MapReduce->debug( 'Cleaning up job keys' );
    
    my $redis = $self->redis;
    my $id    = $self->id;

    $redis->hdel( mapper  => $id );
    $redis->hdel( reducer => $id );
    
    my $keys = $redis->keys($id.'-*');
    
    for my $key (@$keys) {
        $redis->expire($key, 60);
    }
}

sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    while (1) {
        my $reduced = $redis->rpop( $self->id.'-reduced');
        
        if (!defined $reduced) {
            return undef if $self->done;
            sleep 1 if $LOGGING >= $DEBUG;
            next;
        }

        my $value = thaw($reduced);
        
        croak 'Reduced result is undefined?'
            if !defined $value;
        
        $redis->incr( $self->id.'-result-count' );
        
        return $value;
    }
}

sub all_results {
    my ($self) = @_;
    
    my @results;
    
    while (1) {
        my $result = $self->next_result;
        
        if (!defined $result) {
            last if $self->done;
            next;
        }
        
        push @results, $result;
    }
    
    return \@results;
}

sub each_result {
    my ($self, $callback) = @_;
    
    die 'callback required'
        if !$callback;
        
    while (1) {
        my $result = $self->next_result;
        
        if (!defined $result) {
            last if $self->done;
            next;
        }
        
        $callback->($result);
    }
}

sub pmap (&@) {
    my ($mapper, $inputs) = @_;
    
    my $mapper_count = $ENV{MAPREDUCE_PMAP_MAPPERS} // 4;
    
    my @mappers = map { MapReduce::Mapper->new(daemon => 1) } 1 .. $mapper_count;
    my $reducer = MapReduce::Reducer->new(daemon => 1);
    
    my $map_reduce = MapReduce->new(
        name => 'pmap-'.time.$$.int(rand(2**31)),
        
        mapper => sub {
            my ($self, $input) = @_;
            
            local $_ = $input->{value};
            
            my $output = $mapper->(); 
            
            return {
                key    => $input->{key},
                output => $output,
            };
        },
        
        reducer => sub { $_[1] },
    );
    
    my $key = 1;
    
    @$inputs = map { { key => $key++, value => $_ } } @$inputs;
    
    $map_reduce->inputs($inputs);
    
    my $results = $map_reduce->all_results;
    
    my @outputs = map { $_->{output} } sort { $a->{key} <=> $b->{key} } @$results;
    
    return \@outputs;
}

1;

