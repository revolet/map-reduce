package MR::Reducer;
use Moo;
use Storable qw(nfreeze thaw);

with 'MR::Redis';
with 'MR::Cache';
with 'MR::Daemon';

sub run {
    my ($self) = @_;
    
    MR->info( "Reducer $$ started." );
    
    my $redis = $self->redis;
    
    my %reducer;
    
    while (1) {
        my $names = $redis->hkeys('reducer');
                
        for my $name (@$names) {
            if ( !$reducer{$name} ) {
                my $code = $redis->hget( reducer => $name );
                
                MR->debug( "Got reducer for %s: %s", $name, $code );
                
                local $@;
                
                {                
                    $reducer{$name} = eval 'my $sub = sub ' . $code;
                    
                    die "Failed to compile reducer for $name: $@"
                        if $@;
                }
            }
            
            MR->debug("Running reducer for %s", $name);
            
            $self->_run_reducer($name, $reducer{$name});
        }
    }
}

sub _run_reducer {
    my ($self, $name, $reducer) = @_;

    my $redis = $self->redis;
    
    if ($redis->llen( $name.'-mapped' ) == 0) {
        sleep 0.1;
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

