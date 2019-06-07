#!/usr/bin/perl
$|++;

#use Daemon;
require Daemon || die "It appears that the DBI module is not installed! aborting.\n";


my $daemon = Daemon->new_with_options();

my ($command) = @{$daemon->extra_argv};
defined $command || die "No command specified";

$daemon->start   if $command eq 'start';
$daemon->status  if $command eq 'status';
$daemon->restart if $command eq 'restart';
$daemon->stop    if $command eq 'stop';

warn($daemon->status_message);
exit($daemon->exit_code);

