package MR::Cache;
use Moo::Role;
use Storable qw(nfreeze thaw);

requires qw( redis );

sub cache {
    my ($self, %args) = @_;
    
    my $key       = $args{key}     // die 'key required';
    my $field     = $args{field}   // die 'field required';
    my $timeout   = $args{timeout} // 600;
    my $generator = $args{generator};
    
    $key = 'mr-cache-'.$key;
    
    my $value;
    
    if ( $generator && !$self->redis->hexists($key => $field) ) {
        $value = $generator->();
        $self->redis->hset( $key => ( $field => nfreeze($value) ) );
    }
    else {
        $value = $self->redis->hget($key => $field);
        $value = defined $value ? thaw($value) : undef;
    }
    
    $self->redis->expire($key, $timeout);
    
    return $value;
}

1;

