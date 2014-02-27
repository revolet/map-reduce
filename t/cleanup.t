#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis;
use Try::Tiny;
use MapReduce;

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

my @procs = map { MapReduce::Process->new()->start() } 1 .. 5;

my @pids = map { $_->child_pid } @procs;

ok kill 0 => $_ for @pids;

$mr->inputs([ map { { key => $_ } } 1 .. 50 ]);

@procs = ();

ok !kill 0 => $_ for @pids;

my $id = $mr->id;

ok !$mr->redis->exists('mr-commands-'.$_),   "mr-commands-$_ doesn't exist" for @pids;
ok !$mr->redis->exists($id.'-input-count'),  "$id-input-count doesn't exist";
ok !$mr->redis->exists($id.'-inputs'),       "$id-inputs doesn't exist";
ok !$mr->redis->exists($id.'-mapper'),       "$id-mapper doesn't exist";
ok !$mr->redis->exists($id.'-done'),         "$id-done doesn't exist";
ok !$mr->redis->exists($id.'-mapped'),       "$id-mapped doesn't exist";
ok !$mr->redis->exists($id.'-mapped-count'), "$id-mapped-count doesn't exist";

ok !$mr->redis->scard('mr-inputs'), "mr-inputs set is empty";

done_testing;

