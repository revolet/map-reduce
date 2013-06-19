use inc::Module::Install;

name     'MR';
abstract 'MR is a simple Map-Reduce platform that uses Perl 5 and Redis.';
author   'James Barton <james@revolet.com>';
version  '0.12';
license  'MIT';

requires 'Moo'            => 0;
requires 'Redis::hiredis' => 0;


test_requires 'Test::Deep' => 0;
test_requires 'Test::More' => 0;

WriteAll();

