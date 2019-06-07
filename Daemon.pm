package Daemon;

use Moose;
use warnings;
use strict;
use Net::SNMP;
use POSIX;
use bigint;
use threads;
use Net::Ping;

# We need the DBI Perl modules:
require DBI || die "It appears that the DBI module is not installed! aborting.\n";

# vars
my %snmp_part_MIB_pre_in = ( 'SW-VDS' => '.1.3.6.1.4.1.8072.1.3.2.3.1.2.8.86.68.83.', 'SW' => '.1.3.6.1.2.1.31.1.1.1.6.');
my %snmp_part_MIB_post_in = ( 'SW-VDS' => '.45.105.110', 'SW' => '');
my %snmp_part_MIB_pre_out = ( 'SW-VDS' => '.1.3.6.1.4.1.8072.1.3.2.3.1.2.9.86.68.83.', 'SW' => '.1.3.6.1.2.1.31.1.1.1.10.');
my %snmp_part_MIB_post_out = ( 'SW-VDS' =>'.45.111.117.116', 'SW' => '');

my $debug = 1;
my $logfile = "./get_traf_new.log";

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
$year = $year + 1900;
$mon = $mon + 1;
my $my_unix_time = time;


with qw(MooseX::Daemonize);

after start => sub {
        my $self = shift;
        return unless $self->is_daemon;
        # after fork
        #for(;;){
	main();
        #}
 };
 
# subs

sub ping_server
{
	my $ip = @_;
	my $p = Net::Ping->new();
	if($p->ping($ip)){
		return '1';
	}
	return '0';
}

sub logit {
	my $text = shift;
	if ($debug) {
		open (LOG,">>$logfile") or die "Cannot open log file \"$logfile\"!";
		print LOG "$text\n";
		close (LOG);
	}
}

sub check_mysql {
	logit("Enter subroutine check_mysql");
	my($dbh) = @_; 
	
	logit("Creating table in DB for snmp stats");
	# Создаём в БД таблицу для хранения итогов
	my $query = "CREATE TABLE IF NOT EXISTS `" . $year . "_SNMP_test` (`unic_id` INT(12) NOT NULL AUTO_INCREMENT , `server_id` INT(10) NOT NULL , `month` int(8) NOT NULL , `day` INT(32) NOT NULL , `traff_in` BIGINT NOT NULL , `traff_out` BIGINT NOT NULL , `last_change` INT(255) NOT NULL, PRIMARY KEY (`unic_id`), INDEX (`server_id`, `month`, `day`))";
	my $sth = $dbh -> do("$query") or die;
	
	logit("Creating table for tmp stats of snmp");
	# Создаём в БД таблицу для хранения временных данных
	$query = "CREATE TABLE IF NOT EXISTS `SNMP_tmp_table_test` (`server_id` INT(255) NOT NULL, `unix_time` INT(255) NOT NULL, `traff_in_absolute` BIGINT NOT NULL, `traff_out_absolute` BIGINT NOT NULL, INDEX (`server_id`))";
	$sth = $dbh->do("$query") or die;
	#$sth->fihish();
}

sub dbConnect{
	logit("Enter subroutine dbConnect");

	my($database, $username, $password, $dbhost) = @_;

	# Attempt to make connection to the database
	my $dsn = "dbi:mysql:database=".$database.":host=".$dbhost.":port=3306";
	my $dbh = DBI->connect ($dsn, $username, $password);
	unless ($dbh) {
		logit("Error message is ~$DBI::errstr~");
	} else {
		logit("Connect OK.");
	}
	return ($dbh);
}

sub get_switches
{
	my ($dbh) = @_;
	logit("Enter subroutine get_switches");
	my $sql = "";
	#my $sth = $dbh -> prepare($sql);
	#$sth->execute() ;
	my $sth = $dbh->prepare($sql); 
	logit("after $sql was prepared for exec");
	$sth->execute();
	logit("before getting list of switches");
	#my $ips = $sth->fetchall_hashref('dev_ip');
	#$sth->fihish();
	my $ips = $sth->fetchall_arrayref();
	logit("Success to get switches");
	return ($ips);
} 

sub open_snpm_session
{
	#The default timeout is 5.0 seconds.
	my($hostname, $dev_password) = @_;
	my ($session,$error)=Net::SNMP->session(-hostname => $hostname, -community => $dev_password, -version => 'v2c', -timeout  => "1", -retries   => 1,); # открываем сессию SNMP
		unless($session) { logit( "Can not connect to SNMP"); exit; };
	return $session;
}

sub get_switch_info
{
	my ($ip,$database, $username, $password, $dbhost) = @_;
	logit( "getting sw data for $ip");
	my $dbh = dbConnect($database, $username, $password, $dbhost);
	my $sth = $dbh -> prepare("select s.server_id, s.name, d.dev_type, i.port, i.sw_index from servers as s left join interfaces as i on ( i.server_id = s.server_id and i.int_id = s.int_active ) left join devices as d on (i.device_id = d.dev_id) where i.device_id<>0 and s.status<>3 and ( d.dev_type='SW' or d.dev_type='SW-VDS') and d.dev_ip=? order by s.server_id");
	$sth->execute($ip);
	my $sw_info = $sth->fetchall_arrayref();
	#$sth->fihish();
	$dbh->disconnect();
	#returning hash to results
	return ($sw_info); 
}

# лезем в БД с целью достать предыдущие значения счётчиков, для начала достаём максимальный UNIX TIME для этого интерфейса
sub get_unix_time
{
	logit("Entering in sub get_unix_time");
	my($server_id,$database, $username, $password, $dbhost) = @_;
	my $dbh = dbConnect($database, $username, $password, $dbhost);
	my $sth2 = $dbh->prepare("select max(unix_time) from SNMP_tmp_table where server_id = ?");
	$sth2->execute($server_id);
	my ($unix_time) = $sth2->fetchrow_array;
	#$sth2->fihish();
	$dbh->disconnect();
	return $unix_time;
}

# достаём данные от предыдущего съёма статистики
sub get_abs_in_out_traff
{
	my ($server_id, $unix_time,$database, $username, $password, $dbhost) = @_;
	my $dbh = dbConnect($database, $username, $password, $dbhost);
	my $sth2 = $dbh->prepare("select traff_in_absolute, traff_out_absolute from SNMP_tmp_table where server_id = ? and unix_time = ?");
	$sth2->execute($server_id, $unix_time);
	my ($traff_in_absolute, $traff_out_absolute) = $sth2->fetchrow_array;
	#$sth2->fihish();
	$dbh->disconnect();
	return ($traff_in_absolute, $traff_out_absolute);
}

sub update_traffic_table
{
	logit("Entring in sub update_traffic_table");
	my ($server_id, $traff_in, $traff_out, $delta_traff_in, $delta_traff_out,$database, $username, $password, $dbhost) = @_;
	my $dbh = dbConnect($database, $username, $password, $dbhost);
	
	my $sth2 = $dbh->prepare("insert into SNMP_tmp_table_test(server_id,unix_time,traff_in_absolute,traff_out_absolute) values(?, ?, ?, ?)");
	$sth2->execute($server_id, $my_unix_time, $traff_in, $traff_out);

	$sth2 = $dbh->prepare("select count(*) from ".$year."_SNMP_test where server_id = ? and month = ? and day = ?");
	$sth2->execute($server_id, $mon, $mday);
	my $cnt = $sth2->fetchrow_array;

	if ( $cnt == 0 ){
		$sth2 = $dbh->prepare("insert into ".$year."_SNMP_test(server_id, month, day, traff_in, traff_out, last_change) values(?, ?, ?, '0', '0', ?) ");
		$sth2->execute($server_id, $mon, $mday, $my_unix_time);
	}else{
		$sth2 = $dbh->prepare("update ".$year."_SNMP_test set traff_in=traff_in+$delta_traff_in, traff_out=traff_out+$delta_traff_out, last_change = ? where server_id = ? and month = ? and day = ?");
		$sth2->execute($my_unix_time, $server_id, $mon, $mday)
	}
	
	$sth2->fihish();
	$dbh->disconnect();
}		

sub median
{
	my @vals = sort {$a <=> $b} @_;
	my $len = @vals;
	if($len%2) {#odd?
		return $vals[int($len/2)];
	}
	else #even
	{
		return ($vals[int($len/2)-1] + $vals[int($len/2)])/2;
	}
}
		
sub main {
while(1){
	logit("Enter subroutine main");
	my @ThreadPool;
	my $database = $vars{'db_name'};
	my $username = $vars{'db_username'};
	my $password = $vars{'db_password'};
	my $dbhost = $vars{'db_host'};

	my $dbh = dbConnect($database, $username, $password, $dbhost);
	check_mysql($dbh);
	#my %ips = get_switches($dbh);
	
	my $ips = get_switches($dbh);
	$dbh->disconnect();
	my $i = 0;
	foreach my $row (@$ips) {
		my ($dev_ip,$pass) = @$row;
		#my $p = ping_server($dev_ip);
		#if($p eq '1'){
			logit("thread with params was created: $dev_ip => $pass");
			$ThreadPool[$i] = threads->create(\&get_traffic, $dev_ip, $pass,$database, $username, $password, $dbhost);
			$i = $i + 1;
		#}
		#else{
			#logit("host down $dev_ip");
		#}
	}
		
	foreach my $n (@ThreadPool) {
		$n->join();
		logit( "thread N: $n joined");
	}
	
	logit("sleeping 5 minutes");
	sleep 300;
}
}

sub get_traffic {
	my ( $ip, $dev_password,$database, $username, $password, $dbhost ) = @_;
	
	my $session = open_snpm_session($ip, $dev_password);
	my $all = get_switch_info($ip,$database, $username, $password, $dbhost);
	
	foreach my $row (@$all) {
		my ($server_id, $name, $dev_type, $sw_port, $sw_index) = @$row;
		$sw_port = $sw_index if $dev_type eq 'SW';
		my @ports = split(',', $sw_port);
		my ($traff_in, $traff_out) = (0, 0);
		for(my $i = 0; $i < scalar(@ports); $i++) {
			# строим MIB для трафика
			my $snmp_MIB_traff_in = $snmp_part_MIB_pre_in{$dev_type}.$ports[$i].$snmp_part_MIB_post_in{$dev_type};
			my $snmp_MIB_traff_out = $snmp_part_MIB_pre_out{$dev_type}.$ports[$i].$snmp_part_MIB_post_out{$dev_type};
			logit( "ip_addr = $ip - (server_id = $server_id ports = $ports[$i] snmp_traff_in = $snmp_MIB_traff_in )");
			
			my @traff_in_arr = ();
			my @traff_out_arr = ();
			my $subtraff_in1 = $session->get_request("$snmp_MIB_traff_in");
			my $subtraff_out1 = $session->get_request("$snmp_MIB_traff_out");
			push(@traff_in_arr, $subtraff_in1->{"$snmp_MIB_traff_in"} );
			push(@traff_out_arr, $subtraff_out1->{"$snmp_MIB_traff_out"});
			
			sleep 1;
			my $subtraff_in2 = $session->get_request("$snmp_MIB_traff_in");
			my $subtraff_out2 = $session->get_request("$snmp_MIB_traff_out");
			push(@traff_in_arr, $subtraff_in2->{"$snmp_MIB_traff_in"} );
			push(@traff_out_arr, $subtraff_out2->{"$snmp_MIB_traff_out"});
		
			sleep 1;
			my $subtraff_in3 = $session->get_request("$snmp_MIB_traff_in");
			my $subtraff_out3 = $session->get_request("$snmp_MIB_traff_out");
			push(@traff_in_arr, $subtraff_in3->{"$snmp_MIB_traff_in"} );
			push(@traff_out_arr, $subtraff_out3->{"$snmp_MIB_traff_out"});
			my $subtraff_in = median(@traff_in_arr);
			my $subtraff_out = median(@traff_out_arr);
			
			# результаты по траффику
			#$traff_in += $subtraff_in->{"$snmp_MIB_traff_in"};
			#$traff_out += $subtraff_out->{"$snmp_MIB_traff_out"};
			my $traff_in_str = "";
			my $traff_out_str = "";

			foreach my $n (@traff_in_arr) {
				$traff_in_str .= " :$n";
			}
			foreach my $n (@traff_in_arr) {
				$traff_out_str .= " :$n";
			}
			logit("$ip out_traff_str = ".$traff_out_str);
			logit("$ip traff_int str = ".$traff_in_str);
			$traff_in += $subtraff_in;
			$traff_out += $subtraff_out;
			logit("ip_addr = $ip - trf_in = $traff_in, trf_out = $traff_out ");
			logit("ip_addr = $ip: sub_traff_in= $subtraff_in and sub_traf_out= $subtraff_out ");
		}
		
		if ( $traff_in == 0 || $traff_out == 0 ){ next };
		unless(defined $traff_in){ next };
		unless(defined $traff_out){ next };
		logit("ip_addr = $ip - traff_in = $traff_in, traff_out =  $traff_out");
		
		my $unix_time = get_unix_time($server_id,$database, $username, $password, $dbhost);
		
		my ($traff_in_absolute, $traff_out_absolute ) = (0, 0);
		($traff_in_absolute, $traff_out_absolute)  = get_abs_in_out_traff($server_id, $unix_time,$database, $username, $password, $dbhost);
		unless($traff_in_absolute =~ /^[0-9]{1,}$/){ $traff_in_absolute = 0; }
		unless($traff_out_absolute =~ /^[0-9]{1,}$/){ $traff_out_absolute = 0; }
				
		# проверяем, больше, или меньше текущее значение, чем предыдущее, на случай, что счётчики сбрасывались, между измерениями
		my $delta_traff_in = 0;
		my $delta_traff_out = 0;
		
		if($traff_in >= $traff_in_absolute){ $delta_traff_in = $traff_in - $traff_in_absolute; }
		if($traff_out >= $traff_out_absolute){ $delta_traff_out = $traff_out - $traff_out_absolute; }
		my $diff_in = $traff_in - $traff_in_absolute;
		my $diff_out = $traff_out - $traff_out_absolute;
		logit("$ip : diff_in = $diff_in, diff_out = $diff_out");
		
		update_traffic_table($server_id, $traff_in, $traff_out,$delta_traff_in, $delta_traff_out, $database, $username, $password, $dbhost);
	}
	
	$session->close;
}
 
 1; # ok!

