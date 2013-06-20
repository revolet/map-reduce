package MapReduce::Role::Redis;
use Moo::Role;
use Redis::hiredis;

my $redis;

has _redis => (
    is      => 'ro',
    writer  => '_set_redis',
    default => sub { $redis },
);

my $pid = '';

sub redis {
    my ($self) = @_;
    
    return $self->_redis if $pid eq $$;
    
    $redis = Redis::hiredis->new(utf8 => 0);
    
    my $host    = $ENV{MAPREDUCE_REDIS_HOST}    // '127.0.0.1';
    my $port    = $ENV{MAPREDUCE_REDIS_PORT}    // 6379;
    my $db      = $ENV{MAPREDUCE_REDIS_DB}      // 9;
    my $flushdb = $ENV{MAPREDUCE_REDIS_FLUSHDB} // 0;
    
    $redis->connect($host, $port);
    $redis->select($db);
    
    $pid = $$;

    $self->_set_redis($redis);
    
    return $redis;
}

1;

