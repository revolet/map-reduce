package MapReduce::Redis;
use Moo::Role;
use Redis::hiredis;

my $redis = Redis::hiredis->new(utf8 => 0);

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
    
    my $host    = $ENV{MAPREDUCEREDIS_HOST}    // '127.0.0.1';
    my $port    = $ENV{MAPREDUCEREDIS_PORT}    // 6379;
    my $db      = $ENV{MAPREDUCEREDIS_DB}      // 9;
    my $flushdb = $ENV{MAPREDUCEREDIS_FLUSHDB} // 0;
    
    $redis->connect($host, $port);
    $redis->select($db);
    
    $redis->flushdb() if $flushdb;
    
    $pid = $$;

    $self->_set_redis($redis);
    
    return $redis;
}

1;

