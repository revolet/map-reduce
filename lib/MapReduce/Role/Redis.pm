package MapReduce::Role::Redis;
use Moo::Role;
use Redis::hiredis;
use Try::Tiny;
use Time::HiRes;
use MapReduce;

my $redis;

has _redis => (
    is      => 'ro',
    writer  => '_set_redis',
    default => sub { $redis },
);

my $pid = '';

sub redis {
    my ($self) = @_;
    
    return $self->_new_redis if $pid ne $$;
    
    my $redis = $self->_redis;
    
    try {
        if ($redis->ping ne 'PONG') {
            $redis = $self->_new_redis;
        }
    }
    catch {
        MapReduce->debug("Connection to redis closed.  Re-opening.  Error: $_");
        $redis = $self->_new_redis;
    };
    
    return $redis;
}

sub _new_redis {
    my ($self) = @_;
    
    $redis = Redis::hiredis->new(utf8 => 0);
    
    my $host = $ENV{MAPREDUCE_REDIS_HOST} // '127.0.0.1';
    my $port = $ENV{MAPREDUCE_REDIS_PORT} // 6379;
    my $db   = $ENV{MAPREDUCE_REDIS_DB}   // 9;
    
    $redis->connect($host, $port);
    $redis->select($db);
    
    $pid = $$;

    $self->_set_redis($redis);
    
    return $redis;
}

1;

