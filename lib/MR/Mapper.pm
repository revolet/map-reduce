package MR::Mapper;
use Moo;
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

my %mapper;

sub _run {
    my ($self) = @_;
    
    MR->info( "Mapper $$ started." );
    
    my $redis = $self->redis;
    
    while (1) {
        my $names = $redis->hkeys('mapper');
                
        for my $name (@$names) {
            if ( !$mapper{$name} ) {
                my $code = $redis->hget( mapper => $name );
                
                MR->debug( "Got mapper for %s: %s", $name, $code );
                
                local $@;
                
                {                
                    $mapper{$name} = eval 'sub ' . $code;
                    
                    die "Failed to compile mapper for $name: $@"
                        if $@;
                }
            }
            
            $self->_run_mapper($name, $mapper{$name});
        }
    }
}

sub _run_mapper {
    my ($self, $name, $mapper) = @_;
    
    my $redis = $self->redis;
    
    return if $redis->llen( $name.'-input' ) == 0;
    
    $redis->set( $name.'-mapping', 1 );
    
    my $input = $redis->rpop( $name.'-input' );
    
    if (!defined $input) {
        $redis->set( $name.'-mapping', 0);
        return;
    }
    
    my $value = thaw($input);
    
    MR->debug( "Got input '%s' => '%s'", $value->{key}, $value->{value} );
    
    die 'Mapper is undefined? for ' . $$
        if !defined $mapper;
    
    my $mapped = $mapper->($value);
    
    MR->debug( "Mapped is '%s' => '%s'", $mapped->{key}, $mapped->{value} );
    
    $redis->lpush( $name.'-mapped', nfreeze($mapped) );
    
    $redis->set( $name.'-mapping', 0 );
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
    
    MR->info( "Waiting on mapper $pid to stop." );
        
    waitpid $pid, 0;    
    
    MR->info( "Mapper $$ stopped." );
    
    $? = $status;
}

1;

