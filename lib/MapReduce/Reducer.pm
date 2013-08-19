package MapReduce::Reducer;
use Moo;
use Storable qw(nfreeze thaw);
use Time::HiRes qw(usleep);
use List::MoreUtils qw(any);
use MapReduce;

has reducers => (
    is      => 'ro',
    default => sub { {} },
);

with 'MapReduce::Role::Daemon';
with 'MapReduce::Role::Redis';

sub setup {
    my ($self) = @_;
    
    MapReduce->info( "Reducer $$ started." );
    
    $0 = 'mr.reducer';
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my $ids = $redis->hkeys('reducer');
    
    my $work = 0;
            
    for my $id (@$ids) {
        if ( !exists $self->reducers->{$id} ) {
            my $code = $redis->hget( reducer => $id );
            
            next if !$code;
            
            MapReduce->debug( "Got reducer for %s: %s", $id, $code );
            
            local $@;
            
            {                
                $self->reducers->{$id} = eval $code;
                
                die "Failed to compile reducer for $id: $@"
                    if $@;
            }
        }
        
        $work += $self->_run_reducer($id, $self->reducers->{$id});
    }
    
    usleep 10_000 if !$work;
}

sub _run_reducer {
    my ($self, $id, $reducer) = @_;

    my $redis = $self->redis;
    
    if ($redis->llen( $id.'-mapped' ) == 0) {
        return 0;
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
    
    return 1;
}

1;

