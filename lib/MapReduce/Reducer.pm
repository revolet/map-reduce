package MR::Reducer;
use Moo;
use Storable qw(nfreeze thaw);

with 'MR::Redis';
with 'MR::Cache';
with 'MR::Daemon';

has reducers => (
    is      => 'ro',
    default => sub { {} },
);

sub run_loop {
    my ($self) = @_;
    
    MR->info( "Reducer $$ started." );
    
    while (1) {
        $self->run();
    }
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my $names = $redis->hkeys('reducer');
            
    for my $name (@$names) {
        if ( !$self->reducers->{$name} ) {
            my $code = $redis->hget( reducer => $name );
            
            next if !$code;
            
            MR->debug( "Got reducer for %s: %s", $name, $code );
            
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
    
    $redis->set( $name.'-reducing', 1 );
    
    my @values;
    
    while (1) {
        my $mapped = $redis->rpop( $name.'-mapped' );

        last if !defined $mapped;    
        
        my $value = thaw($mapped);
    
        MR->debug( "Got mapped '%s'", $value->{key} );
        
        push @values, $value;
    }
    
    if (@values > 0) {
        my $reduced = $reducer->($self, \@values);
        
        MR->debug( "Reduced is '%s'", $_->{key} )
            for @$reduced;

        $redis->lpush( $name.'-reduced', nfreeze($_) )
            for @$reduced;
    }

    $redis->set( $name.'-reducing', 0 );
}

1;

