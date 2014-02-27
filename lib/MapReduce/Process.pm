package MapReduce::Process;
use Moo;
use Try::Tiny;
use MapReduce;
use MapReduce::Mapper;

has parent_pid => (
    is      => 'rw',
    default => '',
);

has child_pid => (
    is      => 'rw',
    default => '',
);

has max_iterations => (
    is      => 'ro',
    default => 1000,
);

has max_memory => (
    is      => 'ro',
    default => 256,
);

with 'MapReduce::Role::Redis';

sub start {
    my ($self) = @_;
    
    $self->parent_pid($$);
    
    my $pid = fork;
    
    die 'Unable to fork child process'
        if !defined $pid;
        
    $self->child_pid($pid || $$);
        
    return $self if $pid > 0;  
    
    my $should_run = 1;
    
    $SIG{TERM} = $SIG{INT} = sub {
        $should_run = 0;
    };
    
    my $mapper = MapReduce::Mapper->new();
    
    while ($should_run) {
        try {
            $should_run = 0 if !$mapper->run();
        }
        catch {
            MapReduce->warn("Mapper $$ encountered an error: $_");
            sleep 1;
        };
    }
    
    exit 0;
}

sub is_running {
    my ($self) = @_;
    
    return 0 if !$self->child_pid;
    
    return 1 if $self->child_pid eq $$;
    
    return kill 0 => $self->child_pid;
}

sub stop {
    my ($self) = @_;
    
    return if $$ ne $self->parent_pid;
       
    return if !$self->is_running;
    
    MapReduce->info( 'Sending TERM to child %s.', $self->child_pid );
    
    $self->redis->rpush('mr-commands-' . $self->child_pid, 'stop');
    
    kill 'TERM' => $self->child_pid;

    local $SIG{ALRM} = sub {
        MapReduce->info( 'Sending KILL to unresponsive child %s.', $self->child_pid );
        
        kill 'KILL' => $self->child_pid;
    };
    
    alarm 3;
    
    local $?;
    
    waitpid $self->child_pid, 0;
    
    alarm 0;
    
    MapReduce->info( 'Child %s exited.', $self->child_pid )
        if !kill( 0 => $self->child_pid );
}

sub DEMOLISH {
    my ($self) = @_;
    
    return if $$ ne $self->parent_pid;
    
    $self->stop();
}

1;

