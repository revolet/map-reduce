package MapReduce::Process;
use Moo;
use Try::Tiny;
use POSIX qw(:sys_wait_h);
use IO::File;
use Time::HiRes qw(sleep);
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
    default => 384,
);

has warn => (
    is      => 'ro',
    default => 1,
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
    
    MapReduce->info( 'Mapper process %s started.', $$ );
    
    my $should_run = 1;
    
    $SIG{TERM} = $SIG{INT} = sub {
        $should_run = 0;
    };
    
    my $mapper = MapReduce::Mapper->new();
    
    my $iterations = 0;
    
    while ($should_run) {
        try {
            $should_run = 0 if !$mapper->run();
        }
        catch {
            MapReduce->warn("Mapper $$ encountered an error: $_")
                if $self->warn;
                
            sleep 1;
        };
        
        $should_run = 0 if ++$iterations >= $self->max_iterations;
        
        $should_run = 0 if $self->_memory_usage > $self->max_memory;
    }
    
    MapReduce->info('Mapper %s is exiting after %s iterations using %s MB memory', $$, $iterations, $self->_memory_usage);
    
    exit 0;
}

sub is_running {
    my ($self) = @_;
    
    return 0 if !$self->child_pid;
    
    return 1 if $self->child_pid eq $$;
    
    waitpid $self->child_pid, WNOHANG;
    
    return kill 0 => $self->child_pid;
}

sub stop {
    my ($self) = @_;
    
    return if $$ ne $self->parent_pid;
       
    return if !$self->is_running;
    
    MapReduce->info( 'Sending TERM to child %s.', $self->child_pid );
    
    $self->redis->rpush('mr-commands-' . $self->child_pid, 'stop');
    $self->redis->expire('mr-commands-' . $self->child_pid, 60*60);
    
    kill 'TERM' => $self->child_pid;
    
    for (1..3000) {
        waitpid $self->child_pid, WNOHANG;
        
        if (!kill 0 => $self->child_pid) {
            MapReduce->info( 'Child %s exited.', $self->child_pid );
            return;
        }
        
        sleep 0.01;
    }

    MapReduce->info( 'Sending KILL to unresponsive child %s.', $self->child_pid );
        
    kill 'KILL' => $self->child_pid;
    
    for (1..300) {
        waitpid $self->child_pid, WNOHANG;
        
        if (!kill 0 => $self->child_pid) {
            MapReduce->info( 'Child %s exited.', $self->child_pid );
            return;
        }
        
        sleep 0.01;
    }
    
    MapReduce->info( 'Child %s could not be terminated.', $self->child_pid );
}

sub _memory_usage {
    my ($self) = @_;
    
    my $fh = IO::File->new('/proc/self/status', 'r');
    
    my $status = join q{}, $fh->getlines();
    
    $fh->close();
    
    my ($KB) = $status =~ m{ VmRSS: \s+ (\d+) }xms;
    
    return int( $KB / 1024 );
}

sub DEMOLISH {
    my ($self) = @_;
    
    if ($$ eq $self->child_pid) {
        $self->redis->del('mr-commands-'.$$);
    }
    else {    
        $self->stop();
    }
}

1;

