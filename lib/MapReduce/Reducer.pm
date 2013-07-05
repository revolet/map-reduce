package MapReduce::Reducer;
use Moo;
use Storable qw(nfreeze thaw);
use List::MoreUtils qw(any);

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
    
    my $ids = $redis->hkeys('reducer');
            
    for my $id (@$ids) {
        if ( !exists $self->reducers->{$id} ) {
            my $code = $redis->hget( reducer => $id );
            
            next if !$code;
            
            MapReduce->debug( "Got reducer for %s: %s", $id, $code );
            
            local $@;
            
            {                
                $self->reducers->{$id} = eval 'my $sub = sub ' . $code;
                
                die "Failed to compile reducer for $id: $@"
                    if $@;
            }
        }
        
        $self->_run_reducer($id, $self->reducers->{$id});
    }
}

sub _run_reducer {
    my ($self, $id, $reducer) = @_;

    my $redis = $self->redis;
    
    if ($redis->llen( $id.'-mapped' ) == 0) {
        return;
    }
    
    $redis->incr( $id.'-reducing' );
    
    my @values;
    
    while (1) {
        my $mapped = $redis->rpop( $id.'-mapped' );

        last if !defined $mapped;    
        
        my $value = thaw($mapped);
    
        MapReduce->debug( "Got mapped '%s'", $value->{key} );
        
        $redis->incr( $id.'-reduced-count' );
        
        push @values, $value;
    }
    
    if (@values > 0) {
        my $reduced = $reducer->($self, \@values);
        
        die 'Reduced value is defined but has no key?'
            if any { defined $_ && !defined $_->{key} } @$reduced;
        
        MapReduce->debug( "Reduced is '%s'", $_->{key} )
            for grep { defined $_ } @$reduced;

        $redis->lpush( $id.'-reduced', nfreeze($_) )
            for grep { defined $_ } @$reduced;
    }

    $redis->decr( $id.'-reducing' );
}

1;

