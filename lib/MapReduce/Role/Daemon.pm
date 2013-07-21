package MapReduce::Role::Daemon;
use Moo::Role;

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

sub BUILD {
    my ($self) = @_;
    
    return if !$self->daemon;
    
    $self->parent_pid($$);
    
    my $pid = fork;
    
    die 'Unable to fork child process'
        if !defined $pid;

    if ($pid == 0) {       
        $self->run_loop();
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
    
    return if $$ ne $self->parent_pid;
       
    return if !$self->is_running;

    kill 'KILL' => $self->child_pid;
    
    MapReduce->info( "Waiting on worker %s to stop.", $self->child_pid );
        
    local $?;

    waitpid $self->child_pid, 0;
    
    MapReduce->info( "Worker %s stopped.", $self->child_pid );
}

sub DEMOLISH {
    my ($self) = @_;
    
    $self->stop();
}

1;

