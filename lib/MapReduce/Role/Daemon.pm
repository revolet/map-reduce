package MapReduce::Role::Daemon;
use Moo::Role;

has daemon => (
    is      => 'ro',
    default => sub { 0 },
);

has pid => (
    is => 'rw',
);

my $parent = -1;

my @pids;

sub BUILD {
    my ($self) = @_;
    
    return if !$self->daemon;
    
    $parent = $$;
    
    $SIG{TERM} = sub { exit 0 };

    my $pid = fork;
    
    die 'Unable to fork child process'
        if !defined $pid;
    
    if ($pid == 0) {
        $self->run_loop();
        exit 0;
    }
    
    $self->pid($pid);
    
    push @pids, $pid;
}

sub is_running {
    my ($self) = @_;
    
    return 0 if !$self->pid;
    
    return kill 0 => $self->pid;
}

sub DEMOLISH {
    my ($self) = @_;
    
    REAPER($self->pid)
        if $$ == $parent;
}

END {
    if ($$ == $parent) {
        REAPER($_) for @pids;
    }
}

sub REAPER {
    my ($pid) = @_;
    
    # Save status to interfering with test suite
    my $status = $?;
    
    return if !kill( 0 => $pid );

    kill 'TERM' => $pid;
    
    MapReduce->info( "Waiting on mapper $pid to stop." );
        
    waitpid $pid, 0;    
    
    MapReduce->info( "Mapper $$ stopped." );
    
    $? = $status;
}

1;

