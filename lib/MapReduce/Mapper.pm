package MapReduce::Mapper;
use Moo;
use Storable qw(nfreeze thaw);
use Time::HiRes qw(usleep);
use Try::Tiny;
use MapReduce;

has mappers => (
    is      => 'ro',
    default => sub { {} },
);

with 'MapReduce::Role::Daemon';
with 'MapReduce::Role::Redis';

sub setup {
    my ($self) = @_;
    
    MapReduce->info( "Mapper $$ started." );
    
    $0 = 'mr.mapper';
}

sub run {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    my ($key, $input) = $redis->brpop('mr-inputs', 'mr-commands-'.$$, 1);
    
    return 1 if !$key || !$input;
    
    return 0 if $key eq 'mr-commands-'.$$ && $input eq 'exit';
    
    my $value = thaw($input);
    
    my $id = $value->{_id};
    
    MapReduce->debug( "Got input '%s'", $id );
    
    if ( !exists $self->mappers->{$id} ) {
        my $code = $redis->get($id.'-mapper');
        
        MapReduce->debug( "Got mapper for %s in process %s: %s", $id, $$, $code );
        
        local $@;
        
        {                
            $self->mappers->{$id} = eval $code;
            
            die "Failed to compile mapper for $id: $@"
                if $@;
        }
    }
    
    my $mapped;
    
    try {
        $mapped = $self->mappers->{$id}->($self, $value);
    }
    catch {
        die "Mapper for $id died: $_";
    };
    
    return 1 if !defined $mapped;
        
    die 'Mapped value is defined but has no key?'
        if !defined $mapped->{key};
    
    $mapped->{_id} = $id;
    
    $redis->lpush( $id.'-mapped', nfreeze($mapped) );
    $redis->expire( $id.'-mapped', 60*60*24 );
    $redis->incr( $id.'-mapped-count' );
    $redis->expire( $id.'-mapped-count', 60*60*24 );
    
    MapReduce->debug( "Mapped is '%s'", $mapped->{key} );
    
    if ( $redis->get($id.'-mapped-count') >= $redis->get($id.'-input-count')) {
        MapReduce->debug("Job $id is done");
    
        $redis->setex( $id.'-done', 60*60*24, 1 );
    }
    
    return 1;
}

1;

