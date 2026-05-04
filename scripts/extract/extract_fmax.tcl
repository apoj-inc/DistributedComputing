package require ::quartus::project
package require ::quartus::sta

if {$argc < 2} {
    puts stderr "usage: quartus_sta -t extract_fmax.tcl <project_name> <out_json>"
    qexit -error
}

set project_name [lindex $argv 0]
set out_json [lindex $argv 1]

set project_is_open 0
set timing_netlist_created 0

proc json_escape {s} {
    regsub -all {\\} $s {\\\\} s
    regsub -all {"}  $s {\\"} s
    regsub -all {\n} $s {\\n} s
    regsub -all {\r} $s {\\r} s
    regsub -all {\t} $s {\\t} s
    return $s
}

proc cleanup {} {
    global project_is_open timing_netlist_created
    if {$timing_netlist_created} {
        catch {delete_timing_netlist}
        set timing_netlist_created 0
    }
    if {$project_is_open} {
        catch {project_close}
        set project_is_open 0
    }
}

if {[catch {project_open $project_name} err]} {
    puts stderr "failed to open project '$project_name': $err"
    qexit -error
}
set project_is_open 1

if {[catch {create_timing_netlist} err]} {
    puts stderr "failed to create timing netlist: $err"
    cleanup
    qexit -error
}
set timing_netlist_created 1

if {[catch {read_sdc} err]} {
    puts stderr "failed to read SDC: $err"
    cleanup
    qexit -error
}

if {[catch {update_timing_netlist} err]} {
    puts stderr "failed to update timing netlist: $err"
    cleanup
    qexit -error
}

if {[catch {set clocks [get_clock_fmax_info]} err]} {
    puts stderr "failed to get clock fmax info: $err"
    cleanup
    qexit -error
}

if {[catch {set fh [open $out_json w]} err]} {
    puts stderr "failed to open output file '$out_json': $err"
    cleanup
    qexit -error
}

puts $fh "\["

for {set i 0} {$i < [llength $clocks]} {incr i} {
    set item [lindex $clocks $i]
    set comma ","
    if {$i == [expr {[llength $clocks] - 1}]} {
        set comma ""
    }

    puts $fh "  {"
    puts $fh [format {    "raw": "%s"} [json_escape $item]]
    puts $fh [format "  }%s" $comma]
}

puts $fh "\]"
close $fh

cleanup
qexit -success
