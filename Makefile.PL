use inc::Module::Install;

name     'MapReduce';
abstract 'MapReduce is a simple Map-Reduce platform that uses Perl 5 and Redis.';
author   'James Barton <james@revolet.com>';
version  '0.201';
license  'MIT';

requires 'Data::Dump::Streamer' => 0;
requires 'File::Pid' => 0;
requires 'Moo' => 0;
requires 'Redis' => 0;
requires 'Try::Tiny' => 0;

test_requires 'Test::Deep' => 0;
test_requires 'Test::More' => 0;

auto_install();

WriteAll();

