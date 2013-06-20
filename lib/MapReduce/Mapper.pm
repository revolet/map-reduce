package MR::Mapper;
use Moo;
use Storable qw(nfreeze thaw);

has mappers => (
    is      => 'ro',
    default => sub { {} },
);

with 'MR::Redis';
with 'MR::Cache';
with 'MR::Daemon';

sub run_loop {
    my ($self) = @_;
    
    MR->info( "Mapper $$ started." );

    while (1) {
        $self->run();
    }
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my $names = $redis->hkeys('mapper');
    
    for my $name (@$names) {
        if ( !exists $self->mappers->{$name} ) {
            my $code = $redis->hget( mapper => $name );
            
            next if !$code;
            
            MR->debug( "Got mapper for %s in process %s: %s", $name, $$, $code );
            
            local $@;
            
            {                
                $self->mappers->{$name} = eval 'sub ' . $code;
                
                die "Failed to compile mapper for $name: $@"
                    if $@;
            }
        }
        
        $self->_run_mapper($name, $self->mappers->{$name});
    }
}

sub _run_mapper {
    my ($self, $name, $mapper) = @_;
    
    my $redis = $self->redis;
    
    if ($redis->llen( $name.'-input' ) == 0) {
        return;
    }
    
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
        
        MR->debug( "Mapped is '%s'", $mapped->{key} );
    }
    
    $redis->set( $name.'-mapping', 0 );
}

1;

