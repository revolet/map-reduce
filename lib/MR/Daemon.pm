package MR::Daemon;
use Moo::Role;

has daemon => (
    is      => 'ro',
    default => sub { 0 },
);

has pid => (
    is => 'rw',
);

my @pids;

sub BUILD {
    my ($self) = @_;
    
    return if !$self->daemon;

    my $pid = fork;
    
    if ($pid == 0) {
        $self->run();
        exit 0;
    }
    
    $self->pid($pid);
    
    push @pids, $pid;
}

sub DESTROY {
    my ($self) = @_;
    
    REAPER($self->pid);
}

END {
    REAPER($_) for @pids;
}

sub REAPER {
    my ($pid) = @_;
    
    # Save status to interfering with test suite
    my $status = $?;
    
    return if !kill( 0 => $pid );

    kill 'TERM' => $pid;
    
    MR->info( "Waiting on mapper $pid to stop." );
        
    waitpid $pid, 0;    
    
    MR->info( "Mapper $$ stopped." );
    
    $? = $status;
}

1;

