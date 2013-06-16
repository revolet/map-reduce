package MR;
use Moo;
use Storable qw(nfreeze thaw);
use B::Deparse;
use MR::Mapper;
use MR::Reducer;

# Do not change these. Rather, to enable logging,
# change the $LOGGING value to one of these variables.
our $DEBUG = 2;
our $INFO  = 1;
our $NONE  = 0;

# Enable / disable logging.
our $LOGGING = $NONE;

has [ qw( name mapper reducer ) ] => (
    is       => 'ro',
    required => 1,
);

has redis => (
    is       => 'ro',
    lazy     => 1,
    default  => sub { Redis::hiredis->new(utf8 => 0) },
);

sub debug { shift; print STDERR 'DEBUG: ', @_, "\n" if $LOGGING >= $DEBUG }
sub info  { shift; print STDERR 'INFO: ',  @_, "\n" if $LOGGING >= $INFO  }

sub input {
    my ($self, $input) = @_;
    
    my $redis = $self->redis;
    
    $redis->connect('127.0.0.1', 6379);
    $redis->select(9);
    
    $redis->lpush( $self->name.'-input', nfreeze($input) );
    
    MR->debug( "Pushed input '${$input}{key}' => '${$input}{value}'to ".$self->name."-input." );
    
    return $self;
}

sub run {
    my ($self) = @_;
    
    my $deparse = B::Deparse->new();
    my $mapper  = $deparse->coderef2text( $self->mapper  );
    my $reducer = $deparse->coderef2text( $self->reducer );
    my $redis   = $self->redis;
    
    MR->debug( "Mapper is '$mapper'" );
    MR->debug( "Reducer is '$reducer'" );
    
    $redis->hset( mapper  => ( $self->name => $mapper  ) );
    $redis->hset( reducer => ( $self->name => $reducer ) );
    
    return $self;
}

sub done {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    return $redis->llen( $name.'-input'    ) == 0
        && $redis->llen( $name.'-mapping'  ) == 0
        && $redis->llen( $name.'-mapped'   ) == 0
        && $redis->llen( $name.'-reducing' ) == 0
        && $redis->llen( $name.'-reduced'  ) == 0
    ;
}
sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    while (1) {
        return undef if $self->done;

        MR->debug( "Input queue: "    . $redis->llen( $name.'-input'    ) );
        MR->debug( "Mapping queue: "  . $redis->llen( $name.'-mapping'  ) );
        MR->debug( "Mapped queue: "   . $redis->llen( $name.'-mapped'   ) );
        MR->debug( "Reducing queue: " . $redis->llen( $name.'-reducing' ) );
        MR->debug( "Reduced queue: "  . $redis->llen( $name.'-reduced'  ) );

        my $reduced = $redis->brpop( $self->name.'-reduced', 1);
        
        next if !defined $reduced;
        next if !defined $reduced->[1];
        
        my $value = thaw($reduced->[1]);
        
        die 'Reduced result is undefined?'
            if !defined $value;
        
        return $value;
    }
}

1;

