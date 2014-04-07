package MapReduce::Process::Manager;
use Moo;
use POSIX qw(:sys_wait_h setsid);
use Time::HiRes qw(sleep);
use File::Pid;
use MapReduce::Process;
use FindBin qw($Bin);

has max_processes => (
    is      => 'ro',
    default => 1,
);

has daemon => (
    is      => 'ro',
    default => 1,
);

has _procs => (
    is      => 'ro',
    default => sub { [] },
);

sub start {
    my ($self) = @_;
    
    my $pidfile = File::Pid->new({
        file => '/tmp/mapreduce.pid',
    });
    
    if (eval { $self->_pidfile->running }) {
        my $pid = $pidfile->pid;
        
        print STDERR "$0 already running in pid $pid.\n";
        
        exit 1;
    }
    
    fork and return;
    
    if ($self->daemon) {
        setsid;
        
        fork and exit;

        open *STDIN,  '< /dev/null';
        
        qx{mkdir -p $Bin/../logs};
        
        open *STDOUT, "> $Bin/../logs/mapreduce-stdout.log";
        open *STDERR, "> $Bin/../logs/mapreduce-stderr.log";
    }
    
    MapReduce->info('MapReduce Process Manager started.');
    
    my $run = 1;
    
    local $SIG{INT}  = sub { $run = 0 };
    local $SIG{TERM} = sub { $run = 0 };

    $pidfile->pid($$);
    $pidfile->write();
    
    my $procs = $self->_procs;
        
    while ($run) {
        while (@$procs < $self->max_processes) {
            MapReduce->info('Starting a new MapReduce Process.');
            push @$procs, MapReduce::Process->new()->start();
        }

        sleep 1;
        
        waitpid $_->child_pid, WNOHANG for @$procs;
        
        @$procs = grep { $_->is_running } @$procs;
    }
    
    kill 'TERM' => $_->child_pid for @$procs;
    
    while (@$procs > 0) {
        MapReduce->info('Waiting for MapReduce Processes to exit.');
        
        sleep 1;
        
        waitpid $_->child_pid, WNOHANG for @$procs;
        
        @$procs = grep { $_->is_running } @$procs;
    }
    
    exit 0;
}

sub stop {
    my ($self) = @_;
    
    my $pidfile = File::Pid->new({
        file => '/tmp/mapreduce.pid',
    });
    
    my $pid = $pidfile->pid;

    if (!eval { $pidfile->running }) {
        return;
    }
    
    kill 'TERM' => $pid;
    
    for (1..3000) {
        waitpid $pid, WNOHANG;
        
        if (!kill 0 => $pid) {
            MapReduce->info( 'MapReduce Process Manager %s exited.', $pid );
            $pidfile->remove();
            return;
        }
        
        sleep 0.01;
    }

    MapReduce->info( 'Sending KILL to unresponsive MapReduce Process Manager %s.', $pid );
        
    kill 'KILL' => $pid;
    
    for (1..300) {
        waitpid $pid, WNOHANG;
        
        if (!kill 0 => $pid) {
            MapReduce->info( 'MapReduce Process Manager %s exited.', $pid );
            $pidfile->remove();
            return;
        }
        
        sleep 0.01;
    }
    
    MapReduce->info( 'MapReduce Process Manager %s could not be terminated.', $pid );
}

sub DEMOLISH {
    my ($self) = @_;
    
    $self->stop();
}

1;

