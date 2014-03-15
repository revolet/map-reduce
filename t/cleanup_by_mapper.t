#!/usr/bin/env perl
use Test::Most;
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
    name => 'test-cleanup-kill',
    
    mapper => sub { $_[0] },
);

my @procs = map { MapReduce::Process->new(warn => 0, timeout => 1)->start() } 1 .. 5;

my @pids = map { $_->child_pid } @procs;

ok kill 0 => $_ for @pids;

$mr->inputs([ map { { key => $_ } } 1 .. 50 ]);

my $id = $mr->id;

ok !$redis->exists('mr-commands-'.$_),   "mr-commands-$_ doesn't exist" for @pids;

$mr->_set_id('');
$redis->del($id.'-alive');
$mr = undef;

sleep 1;

ok !$redis->exists($id.'-inputs'),       "$id-inputs doesn't exist";
ok !$redis->exists($id.'-input-count'),  "$id-input-count doesn't exist";
ok !$redis->exists($id.'-mapper'),       "$id-mapper doesn't exist";
ok !$redis->exists($id.'-done'),         "$id-done doesn't exist";
ok !$redis->exists($id.'-mapped'),       "$id-mapped doesn't exist";
ok !$redis->exists($id.'-mapped-count'), "$id-mapped-count doesn't exist";

ok !$redis->scard('mr-inputs'), "mr-inputs set is empty";

done_testing;

