package MapReduce::Reducer;
use Moo;
use Storable qw(nfreeze thaw);
use Time::HiRes qw(usleep);
use List::MoreUtils qw(any);
use MapReduce;

1;

