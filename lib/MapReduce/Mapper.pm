package MapReduce::Mapper;
use Moo;
use Storable qw(nfreeze thaw);

has mappers => (
    is      => 'ro',
    default => sub { {} },
);

with 'MapReduce::Role::Daemon';
with 'MapReduce::Role::Redis';

sub run_loop {
    my ($self) = @_;
    
    MapReduce->info( "Mapper $$ started." );

    while (1) {
        $self->run();
    }
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my $ids = $redis->hkeys('mapper');
    
    for my $id (@$ids) {
        if ( !exists $self->mappers->{$id} ) {
            my $code = $redis->hget( mapper => $id );
            
            next if !$code;
            
            MapReduce->debug( "Got mapper for %s in process %s: %s", $id, $$, $code );
            
            local $@;
            
            {                
                $self->mappers->{$id} = eval 'sub ' . $code;
                
                die "Failed to compile mapper for $id: $@"
                    if $@;
            }
        }
        
        $self->_run_mapper($id, $self->mappers->{$id});
    }
}

sub _run_mapper {
    my ($self, $id, $mapper) = @_;
    
    my $redis = $self->redis;
    
    if ($redis->llen( $id.'-input' ) == 0) {
        return;
    }
    
    $redis->incr( $id.'-mapping' );
    
    my $input = $redis->rpop( $id.'-input' );
    
    if (!defined $input) {
        $redis->decr( $id.'-mapping' );
        return;
    }
    
    my $value = thaw($input);
    
    MapReduce->debug( "Got input '%s'", $value->{key} );
    
    die 'Mapper is undefined? for ' . $$
        if !defined $mapper;
    
    my $mapped = $mapper->($self, $value);
    
    if (defined $mapped && defined $mapped->{key}) {
        $redis->lpush( $id.'-mapped', nfreeze($mapped) );
        
        MapReduce->debug( "Mapped is '%s'", $mapped->{key} );
    }
    
    $redis->decr( $id.'-mapping' );
}

1;

