#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis;
use MapReduce;

$ENV{MAPREDUCE_REDIS_DB} = 9;

my $redis = Redis->new(
    encoding  => undef,
    reconnect => 60,
    server    => '127.0.0.1:6379',
);

$redis->select(9);
$redis->flushdb();

# Start up 5 mapreduce processes
my %procs;

for (1..5) {
    $procs{$_} = MapReduce::Process->new(max_memory => 32)->start();
}

my $mr = MapReduce->new(
    name => 'test1',
    
    mapper => sub {
        my ($self, $input) = @_;
        
        $input->{key} *= 2;
        
        $self->{junk} .= '0' x (1024*1024*16);
        
        return $input;
    },
);

ok $_->is_running for values %procs;

# Load up some numbers to feed into our map-reduce functions
$mr->inputs([ map { { key => $_ } } 1 .. 50 ]);

$mr->all_results;

sleep 5;

ok !$_->is_running for values %procs;

done_testing;

