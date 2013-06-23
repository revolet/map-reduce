package MapReduce;
use Moo;
use Storable qw(nfreeze thaw);
use B::Deparse;
use Time::HiRes qw(time);
use Carp qw(croak);
use List::MoreUtils qw(all);
use MapReduce::Mapper;
use MapReduce::Reducer;

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

has [ qw( name mapper reducer ) ] => (
    is       => 'ro',
    required => 1,
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
    
    my $deparse = B::Deparse->new();
    my $mapper  = $deparse->coderef2text( $self->mapper  );
    my $reducer = $deparse->coderef2text( $self->reducer );
    my $redis   = $self->redis;
    
    MapReduce->debug( "Mapper is '%s'",  $mapper );
    MapReduce->debug( "Reducer is '%s'", $reducer );
    
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
    
    MapReduce->debug( "Pushed input '%s' to %s->input.", $input->{key}, $self->id );
    
    return $self;
}

my @pids;

sub input_async {
    my ($self, $sub) = @_;
    
    my $pid = fork;
    
    die 'Unable to fork'
        if !defined $pid;
    
    if ($pid == 0) {
        eval {
            $self->input_start();
            $sub->();
            $self->input_done();
        };
        
        warn $@ if $@;
        exit 0;
    }
    
    push @pids, $pid;
}

END {
    for my $pid (@pids) {
        waitpid $pid, 0;
    }
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
    
    # TODO: This needs to be done in the Mapper and Reducer classes
    if ($done) {
        $redis->hdel( mapper  => $self->id );
        $redis->hdel( reducer => $self->id );
    }
    
    return $done;
}

sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    while (1) {
        my $reduced = $redis->rpop( $self->id.'-reduced');
        
        if (!defined $reduced) {
            return undef if $self->done;
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

1;

