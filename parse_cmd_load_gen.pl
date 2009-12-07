#!/usr/bin/env perl
#
# Grab all Command Load Generation Processing Summaries
# and parse into a database to find the timeline of loads and
# related mission panning directories
#
# A touch file is used to find summaries that are "newer" than the last
# file processed by the tool


use strict;
use warnings;
use IO::All;
use File::stat;
use File::Basename;

use Getopt::Long;
use Ska::Parse_CM_File;
use Ska::Convert qw( date2time );
use Ska::Run;
use Ska::DatabaseUtil qw( sql_insert_hash sql_connect sql_do );
use Chandra::Time;

use Data::Dumper;

my $SKA = $ENV{SKA} || '/proj/sot/ska';
my $task = 'timelines';
my $SKA_DATA = "${SKA}/data/${task}";
my $MP_DIR = "${SKA_DATA}/mp/";

my %opt = ( touch_file => "${SKA_DATA}/sum_files.touch",
	    mp_dir => $MP_DIR,
	    dryrun => 0,
	    dbi => 'sqlite',
	    server => 'db_base.db3',
    );

GetOptions(\%opt,
	   'verbose!',
	   'touch_file=s',
	   'mp_dir=s',
	   'dryrun!',
	   'dbi=s',
	   'server=s',
    );

my $mp_dir = $opt{mp_dir};
my $touch_file_dir = dirname($opt{touch_file});
my $dir_status = run("mkdir -p $touch_file_dir") unless (-d $touch_file_dir);
die("Error making touch_file directory $touch_file_dir") if $dir_status;

# define a global that has the file status of the "touch file" (for modification times)
my $touch_stat;
if ($opt{verbose}){
    print "Updating clgps tables with files newer than ", $opt{touch_file}, "\n";
}

if (-e $opt{touch_file}){    
    $touch_stat = stat($opt{touch_file});
}


my $load_handle;
if ($opt{dbi} eq 'sybase'){
    if ($opt{dryrun}){
	$load_handle = sql_connect('sybase-aca-aca_read');
    }
    else{
	$load_handle = sql_connect('sybase-aca-aca_ops');
    }
}
else{
    $load_handle = sql_connect({ database  => $opt{server},
				 type => 'array',
				 raise_error => 1,  
				 print_error => 1,  
				 DBI_module => 'dbi:SQLite', 
			       });
}


my @files;

# find command load processing summaries newer than the touch file
use POSIX qw( strftime );
my $curr_year = strftime "%Y", gmtime(time());
my $week_glob = '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]';
my $sum_glob = 'C[0-9][0-9][0-9]?[0-9][0-9][0-9][0-9].sum';
YEAR:
for ( my $year = 1999; $year <= $curr_year; $year++){
    my $cmd = "find -L ${mp_dir}/${year}/${week_glob}/ "
	. "-wholename \"*/ofls?/mps/${sum_glob}\" ";
    if (defined $touch_stat){
	$cmd .= " -newer $opt{touch_file}";
	my $touch_year = strftime "%Y", gmtime($touch_stat->mtime());
	next YEAR if $year < $touch_year;
    }
    print "$cmd \n";
    my ($status, @run_files) = run("$cmd");
    unless ($status){
	@files = (@files, @run_files);
    }
}

map {chomp $_} @files;

# for each command load generation summary
SUM_FILE:
for my $file (@files){

    print "Parsing $file" if $opt{verbose};
    
    my $file_stat = stat("$file");

    my ( $week, $loads ) = parse_clgps( $file );
    my ( $dir, $filename);
    if ($file =~ /${mp_dir}(\/\d{4}\/\w{3}\d{4}\/ofls\w?\/)mps\/(C.*\.sum)/){
	$dir = $1;
	$filename =$2;
    }


    # exclude what appear to be weird testing directories
    if ($dir =~ /.*ofls(t|x)/) {
	print "Skipping \n" if $opt{verbose};
	next SUM_FILE;
    }

    if (defined $dir and defined $filename){
	$week->{dir} = $dir;
	$week->{file} = $filename;
	$week->{sumfile_modtime} = $file_stat->mtime; 
	for my $load_ref (@{$loads}){
	    $load_ref->{file} = $filename;
	    $load_ref->{sumfile_modtime} = $week->{sumfile_modtime};
	    my $delete = qq( delete from tl_built_loads where year = $load_ref->{year} 
			     and load_segment = "$load_ref->{load_segment}" 
			     and file = "$filename"
			     and sumfile_modtime = $week->{sumfile_modtime} );
	    sql_do( $load_handle, $delete);
	    sql_insert_hash( $load_handle, 'tl_built_loads', $load_ref );
	}
	
	# only bother to store if it has loads
	sql_do( $load_handle, qq( delete from tl_processing where dir = "$dir" and file = "$filename"));
	sql_insert_hash( $load_handle, 'tl_processing', $week );
	
	my $obsids = get_obsids("${mp_dir}/${dir}", $loads);
	for my $obs_load (keys %{$obsids}){
	    for my $obs_entry (@{$obsids->{$obs_load}}){
		my @ids = split('__', $obs_load);
		my %obs_hash = (
				dir => $dir,
				year => $ids[0],
				load_segment => $ids[1],
				obsid => $obs_entry->{obsid},
				date => $obs_entry->{date},
				); 
		my $obs_delete = qq( delete from tl_obsids where dir = "$dir"
				     and year = $ids[0]
				     and load_segment = "$ids[1]"
				     and obsid = $obs_entry->{obsid}
				     and date = "$obs_entry->{date}");
		sql_do( $load_handle, $obs_delete );
		sql_insert_hash( $load_handle, 'tl_obsids', \%obs_hash );
	    }
	}
    }
    


    # if the touch_stat is earlier than the file time or not defined, update the "touch file"
    if (  ( (defined $touch_stat) and ($touch_stat->mtime < $file_stat->mtime) )
	  or ( not defined $touch_stat ) ) {
	my ($t_status) = run("touch -r $file $opt{touch_file}");
	die("Error touching $opt{touch_file}") if $t_status;
	$touch_stat = stat($opt{touch_file});
    }

    print " ... Done \n" if $opt{verbose};

}



sub get_obsids{
    my $dir = shift;
    my $loads = shift;
    
    my @bs_list = glob("${dir}/*.backstop");
    my $backstop = $bs_list[0];


    my @bs = Ska::Parse_CM_File::backstop( $backstop);
    my %obsids_per_load;
    for my $load (@{$loads}){
	my @obsids;
	my $tstart = date2time($load->{first_cmd_time});
	my $tstop = date2time($load->{last_cmd_time});
	for my $entry (@bs){
	    next unless ( $entry->{time} > $tstart );
	    next unless ( $entry->{cmd} =~ /MP_OBSID/ );
	    my %bs_params = Ska::Parse_CM_File::parse_params($entry->{params});
	    push @obsids, { date => $entry->{date}, obsid => $bs_params{ID}};
	    last if ( $entry->{time} > $tstop );
	}
	my $id = $load->{year} . "__" . $load->{load_segment};
	$obsids_per_load{$id} = \@obsids;
    }
    return \%obsids_per_load;
}


sub parse_clgps {
    my $gps = shift;

    # assume this isn't a replan
    my %week = ( replan => 0 );
		 
    my @loads;
    my $lines = io($gps)->slurp;

    # split the file into paragraphs based on the asterisk separator lines
    my @chunks = split( /\n\*{12}/ , $lines);

    # do the yucky, but maintainable, regex parsing of each paragraph
    for my $piece (@chunks){
	if ( $piece =~ /\*+\sCONTINUITY\/REPLAN\s\*+/){
	    if ( $piece =~ /Replan\/Reopen/){
		$week{replan} = 1;
		if ( $piece =~ /Run\sDirectory:\s\S+\/(C\d{3}:\d{4})\//){
		    $week{replan_cmds} = $1;
		}
		if ($piece =~ /Continuity\sDirectory:\s\S+\/(C\d{3}:\d{4})\//){
		    $week{continuity_cmds} = $1;
		}
	    }
	    else{
		if ($piece =~ /Continuity\sRun\sDirectory:\s\S+\/(C\d{3}:\d{4})\//){
		    $week{continuity_cmds} = $1;
		}
	    }
	}
	if ( $piece =~ /\*+PROCESSING\sVALUES\*+/){
	    if ( $piece =~ /EXECUTION\sBEGIN\sTIME:(\S+)/ ){
		$week{execution_tstart} = $1;
	    }
	    if ( $piece =~ /START\sTIME:\s+(\S+)/ ){
		$week{processing_tstart} = $1;
	    }
	    if ( $piece =~ /STOP\sTIME:\s+(\S+)/ ){
		$week{processing_tstop} = $1;
	    }
	}
	if ( $piece =~ /\*+\sINPUT\sREPLAN\sBCF\sINFORMATION\s\*+/){
	    if ( $piece =~ /TOTAL\sNUMBER\sOF\sCOMMANDS\sREAD\s=\s(\d+)/) {
		$week{bcf_cmd_count} = $1;
	    }
	}
	if ( $piece =~ /\*+\sACTUAL\sPLANNING\sPERIOD\sSTART\/STOP\sTIMES\s\*+/){
	    if ( $piece =~ /ACTUAL\sSTART\sTIME:\s(\S+)/ ){
		$week{planning_tstart} = $1;
	    }
	    if ( $piece =~ /ACTUAL\sSTOP\s\sTIME:\s(\S+)/ ){
		$week{planning_tstop} = $1;
	    }
	    if (defined $week{planning_tstart}){
		if ( $week{planning_tstart} =~ /^(\d{4})/){
		    $week{year} = $1;
		}
	    }
	}
	if ( $piece =~ /\*+LOAD\sGENERATED\*+/){
	    my %load;
	    if ( $piece =~ /Load\sname:\s+(CL\d{3}:\d{4})/){
		$load{load_segment} = $1;
	    }
	    if ( $piece =~ /SCS\sNumber:\s+(\d{3})/){
		$load{load_scs} = $1;
	    }
	    if ( $piece =~ /First\sCommand\sTime:\s+(\S+)/ ){
		$load{first_cmd_time} = $1;
	    }
	    if ( $piece =~ /Last\sCommand\sTime:\s+(\S+)/ ){
		$load{last_cmd_time} = $1;
	    }
	    if ( $load{first_cmd_time} =~ /^(\d{4})/ ){
		$load{year} = $1;
	    }
	    push @loads, \%load;
	}


    }

    # I've put the processing information for the week into a hash
    # and the specifications for the individual loads in an array
    return ( \%week, \@loads );

}



###############################################################
sub obsids_in_segments{
###############################################################

# based on Ska::Parse_CM_File::TLR_load_segments,
# but stores the name of the segment too

    my $tlr_file = shift;

    my @segment_times;
    my @tlr = io($tlr_file)->slurp;

    my @segment_start_lines = grep /START\sOF\sNEW\sOBC\sLOAD,\sCL\d{3}:\d{4}/, @tlr;

    for my $line (@segment_start_lines){
        if ( $line =~ /(\d{4}:\d{3}:\d{2}:\d{2}:\d{2}\.\d{3})\s+START\sOF\sNEW\sOBC\sLOAD,\s(CL\d{3}\:\d{4})/ ){
            my $time = $1;
            my $seg_id = $2;
            push @segment_times, { date => $time, seg_id => $seg_id };
        }

    }

    return @segment_times;
}
