package MapReduce::Reducer;
use Moo;
use Storable qw(nfreeze thaw);

has reducers => (
    is      => 'ro',
    default => sub { {} },
);

with 'MapReduce::Role::Daemon';
with 'MapReduce::Role::Redis';

sub run_loop {
    my ($self) = @_;
    
    MapReduce->info( "Reducer $$ started." );
    
    while (1) {
        $self->run();
    }
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my $names = $redis->hkeys('reducer');
            
    for my $name (@$names) {
        if ( !exists $self->reducers->{$name} ) {
            my $code = $redis->hget( reducer => $name );
            
            next if !$code;
            
            MapReduce->debug( "Got reducer for %s: %s", $name, $code );
            
            local $@;
            
            {                
                $self->reducers->{$name} = eval 'my $sub = sub ' . $code;
                
                die "Failed to compile reducer for $name: $@"
                    if $@;
            }
        }
        
        $self->_run_reducer($name, $self->reducers->{$name});
    }
}

sub _run_reducer {
    my ($self, $name, $reducer) = @_;

    my $redis = $self->redis;
    
    if ($redis->llen( $name.'-mapped' ) == 0) {
        return;
    }
    
    $redis->incr( $name.'-reducing' );
    
    my @values;
    
    while (1) {
        my $mapped = $redis->rpop( $name.'-mapped' );

        last if !defined $mapped;    
        
        my $value = thaw($mapped);
    
        MapReduce->debug( "Got mapped '%s'", $value->{key} );
        
        push @values, $value;
    }
    
    if (@values > 0) {
        my $reduced = $reducer->($self, \@values);
        
        MapReduce->debug( "Reduced is '%s'", $_->{key} )
            for @$reduced;

        $redis->lpush( $name.'-reduced', nfreeze($_) )
            for @$reduced;
    }

    $redis->decr( $name.'-reducing' );
}

1;

