package MR::Reducer;
use Moo;
use Redis::hiredis;
use Storable qw(nfreeze thaw);

has pid => (
    is => 'rw',
);

has redis => (
    is       => 'ro',
    lazy     => 1,
    default  => sub { Redis::hiredis->new(utf8 => 0) },
);

my @pids;

sub run {
    my ($self) = @_;

    my $pid = fork;
    
    if ($pid == 0) {
        $self->redis->connect('127.0.0.1', 6379);
        $self->redis->select(9);

        $self->_run();
        exit 0;
    }
    
    $self->pid($pid);
    push @pids, $pid;
}

my %reducer;

sub _run {
    my ($self) = @_;
    
    MR->info( "Reducer $$ started." );
    
    my $redis = $self->redis;
    
    while (1) {
        my $names = $redis->hkeys('reducer');
                
        for my $name (@$names) {
            if ( !$reducer{$name} ) {
                my $code = $redis->hget( reducer => $name );
                
                MR->debug( "Got reducer for $name: $code" );
                
                local $@;
                
                {                
                    $reducer{$name} = eval 'my $sub = sub ' . $code;
                    
                    die "Failed to compile reducer for $name: $@"
                        if $@;
                }
            }
            
            $self->_run_reducer($name, $reducer{$name});
        }
    }
}

sub _run_reducer {
    my ($self, $name, $reducer) = @_;

    my $redis = $self->redis;
    
    my @values;
    my @mapped;
    
    while (1) {
        my $mapped = $redis->rpoplpush( $name.'-mapped', $name.'-reducing' );

        last if !defined $mapped;    
        
        push @mapped, $mapped;
        
        my $value = thaw($mapped);
    
        MR->debug( "Got mapped '$$value{key}' => '$$value{value}'" );
        
        push @values, $value;
    }
    
    if (@values > 0) {
        my $reduced = $reducer->(\@values);
        
        MR->debug( "Reduced is '$$_{key}' => '$$_{value}'" )
            for @$reduced;

        $redis->lpush( $name.'-reduced', nfreeze($_) )
            for @$reduced;
    }
    
    $redis->lrem( $name.'-reducing', 1, $_ )
        for @mapped;
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
    
    my $status = $?;
    
    return if !kill( 0 => $pid );

    kill 'TERM' => $pid;
    
    MR->info( "Waiting on reducer $pid to stop." );
        
    waitpid $pid, 0;
    
    MR->info( "Reducer $$ stopped." );
    
    $? = $status;
}

1;

