package MapReduce::Role::Daemon;
use Moo::Role;
use Try::Tiny;

has daemon => (
    is      => 'ro',
    default => sub { 0 },
);

has parent_pid => (
    is      => 'rw',
    default => '',
);

has child_pid => (
    is      => 'rw',
    default => '',
);

requires 'run';

sub BUILD {
    my ($self) = @_;
    
    return if !$self->daemon;
    
    $self->parent_pid($$);
    
    my $pid = fork;
    
    die 'Unable to fork child process'
        if !defined $pid;

    if ($pid == 0) {
        my $should_run = 1;
        
        $SIG{TERM} = $SIG{INT} = sub {
            $should_run = 0;
            $self->stop();
        };
        
        while ($should_run) {
            try {
                $should_run = $self->run();
            }
            catch {
                MapReduce->info("Mapper $$ encountered an error: $_");
                sleep 1;
            };
        }
        
        exit 0;
    }
    
    $self->child_pid($pid);
}

sub is_running {
    my ($self) = @_;
    
    return 0 if !$self->child_pid;
    
    return kill 0 => $self->child_pid;
}

sub stop {
    my ($self) = @_;
    
    return if !$self->daemon;
    
    return if $$ ne $self->parent_pid;
       
    return if !$self->is_running;
    
    MapReduce->info( 'Sending TERM to child %s.', $self->child_pid );
    
    $self->redis->rpush('mr-commands-' . $self->child_pid, 'exit');

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
    
    return if !$self->daemon;
    
    $self->stop();
}

1;

