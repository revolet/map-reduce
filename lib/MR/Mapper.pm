package MR::Mapper;
use Moo;
use Storable qw(nfreeze thaw);
use Time::HiRes qw(sleep);

with 'MR::Redis';
with 'MR::Daemon';

sub run {
    my ($self) = @_;
    
    MR->info( "Mapper $$ started." );
    
    my $redis = $self->redis;
    
    my %mapper;
    
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
    
    MR->debug( "Got input '%s'", $value->{key} );
    
    die 'Mapper is undefined? for ' . $$
        if !defined $mapper;
    
    my $mapped = $mapper->($self, $value);
    
    if (defined $mapped && defined $mapped->{key}) {
        $redis->lpush( $name.'-mapped', nfreeze($mapped) );
    }
    
    MR->debug( "Mapped is '%s'", $mapped->{key} );

    
    $redis->set( $name.'-mapping', 0 );
}

1;

