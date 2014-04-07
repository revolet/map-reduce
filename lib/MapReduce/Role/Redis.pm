package MapReduce::Role::Redis;
use Moo::Role;
use Redis;
use Try::Tiny;
use Time::HiRes;
use MapReduce;

my $host = $ENV{MAPREDUCE_REDIS_HOST} // '127.0.0.1';
my $port = $ENV{MAPREDUCE_REDIS_PORT} // 6379;
my $db   = $ENV{MAPREDUCE_REDIS_DB}   // 7;

my $redis;
my $pid = $$;

sub redis {
    return $redis if $redis && $pid eq $$;
    
    $redis = Redis->new(
        encoding  => undef,
        reconnect => 60,
        server    => "$host:$port",
    );

    $redis->select($db);
    
    return $redis;
}

1;

