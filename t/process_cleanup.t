#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis;
use Try::Tiny;
use MapReduce;

$ENV{MAPREDUCE_REDIS_DB} = 9;

my $redis = Redis->new(
    encoding  => undef,
    reconnect => 60,
    server    => '127.0.0.1:6379',
);

$redis->select(9);
$redis->flushdb();

my $mr = MapReduce->new(
    name => 'test-cleanup',
    
    mapper => sub { sleep 10; $_[1] },
);

$mr->inputs([ map { { key => $_ } } 1 .. 5000 ]);

my @procs;

for (1..5) {
    @procs = grep { $_->is_running } @procs;
    
    while (@procs < 10) {
        push @procs, MapReduce::Process->new(max_iterations => int(rand(10))+1)->start();
    }
    
    $_->stop() for @procs;
    
    ok !$_->is_running for @procs;
}

done_testing;

