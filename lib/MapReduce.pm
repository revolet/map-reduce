package MapReduce;
use Moo;
use Storable qw(nfreeze thaw);
use Data::Dump::Streamer qw(Dump);
use Time::HiRes qw(time);
use Carp qw(croak);
use List::MoreUtils qw(all);
use Exporter qw(import);
use MapReduce::Mapper;
use MapReduce::Process;

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
sub warn  { shift->log('WARN',  @_) }

sub log {
    my ($class, $level, $format, @args) = @_;

    $format //= '';
    
    printf STDERR $level.': '.$format."\n", map { defined $_ ? $_ : 'undef' } @args;
}

has [ qw( name mapper ) ] => (
    is       => 'ro',
    required => 1,
);

has id => (
    is => 'lazy',
);

has self_mapper => (
    is      => 'ro',
    default => 1,
);

has mapper_worker => (
    is => 'lazy',
);

with 'MapReduce::Role::Redis';

sub _build_id {
    my ($self) = @_;
    
    my $id = 'mr-'.$self->name . '-' . int(time) . '-' . $$ . '-' . int(rand(2**31));
    
    MapReduce->debug( "ID is '%s'", $id );
    
    return $id;
}

sub _build_mapper_worker {
    my ($self) = @_;
    
    return MapReduce::Mapper->new(block => 0);
}

sub BUILD {
    my ($self) = @_;
    
    my $mapper = ref $self->mapper ? Dump( $self->mapper )->Declare(1)->Out() : $self->mapper;
    my $redis  = $self->redis;
    
    MapReduce->debug( "Mapper is '%s'",  $mapper );
   
    $SIG{INT} = sub { exit 1 }
        if !$SIG{INT};
        
    $SIG{TERM} = sub { exit 1 }
        if !$SIG{TERM};
    
    my $id = $self->id;
    
    $redis->setex( $id.'-mapper', 60*60*24, $mapper );
}

sub inputs {
    my ($self, $inputs) = @_;
    
    my $redis = $self->redis;
    my $id    = $self->id;
    
    $redis->setex( $id.'-input-count', 60*60*24, scalar(@$inputs) );
    
    $redis->sadd( 'mr-inputs', $id );
    
    for my $input (@$inputs) {
        $input->{_id} = $id;
        
        $redis->lpush( $id.'-inputs', nfreeze($input) );
    }
    
    MapReduce->debug( "Pushed %d inputs.", scalar(@$inputs) );
    
    return $self;
}

sub done {
    my ($self) = @_;
    
    return $self->redis->get( $self->id.'-done' );
}

sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $id    = $self->id;
    
    while (1) {
        my $reduced = $redis->rpop( $self->id.'-mapped');
        
        if (!defined $reduced) {
            return undef if $self->done;

            $self->mapper_worker->run()
                if $self->self_mapper;
            
            next;
        }

        my $value = thaw($reduced);
        
        croak 'Reduced result is undefined?'
            if !defined $value;
        
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
    
    my $proc_count = $ENV{MAPREDUCE_PMAP_MAPPERS} // 4;
    
    my @procs = map { MapReduce::Process->new()->start() } 1 .. $proc_count;
    
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
    );
    
    my $key = 1;
    
    @$inputs = map { { key => $key++, value => $_ } } @$inputs;

    $map_reduce->inputs($inputs);

    my $results = $map_reduce->all_results;

    my @outputs = map { $_->{output} } sort { $a->{key} <=> $b->{key} } @$results;
    
    return \@outputs;
}

my $parent_pid = $$;

sub DEMOLISH {
    my ($self) = @_;
    
    return if $$ ne $parent_pid;
    
    my $id    = $self->id;
    my $redis = $self->redis;
    
    $redis->srem('mr-inputs', $id);
    
    $redis->del($id.'-inputs');
    $redis->del($id.'-input-count');
    $redis->del($id.'-mapper');
    $redis->del($id.'-mapped');
    $redis->del($id.'-done');
    $redis->del($id.'-mapped-count');
}

1;

