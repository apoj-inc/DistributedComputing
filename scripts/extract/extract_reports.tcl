package require ::quartus::project
package require ::quartus::report

if {$argc < 2} {
    puts stderr "usage: quartus_sh -t extract_reports.tcl <project_name> <out_json>"
    qexit -error
}

set project_name [lindex $argv 0]
set out_json [lindex $argv 1]

set project_is_open 0
set report_is_loaded 0

proc json_escape {s} {
    regsub -all {\\} $s {\\\\} s
    regsub -all {"}  $s {\\"} s
    regsub -all {\n} $s {\\n} s
    regsub -all {\r} $s {\\r} s
    regsub -all {\t} $s {\\t} s
    return $s
}

proc cleanup {} {
    global project_is_open report_is_loaded
    if {$report_is_loaded} {
        catch {unload_report}
        set report_is_loaded 0
    }
    if {$project_is_open} {
        catch {project_close}
        set project_is_open 0
    }
}

proc leading_spaces_count {s} {
    if {[regexp {^(\s*)} $s -> prefix]} {
        return [string length $prefix]
    }
    return 0
}

if {[catch {project_open $project_name} err]} {
    puts stderr "failed to open project '$project_name': $err"
    qexit -error
}
set project_is_open 1

if {[catch {load_report} err]} {
    puts stderr "failed to load report database: $err"
    cleanup
    qexit -error
}
set report_is_loaded 1

if {[catch {set panel_names [get_report_panel_names]} err]} {
    puts stderr "failed to enumerate report panels: $err"
    cleanup
    qexit -error
}

set matched_panels {}
set entity_rows {}

foreach panel $panel_names {
    if {![string match "*Resource Utilization by Entity*" $panel]} {
        continue
    }

    lappend matched_panels $panel

    if {[catch {set panel_id [get_report_panel_id $panel]} err]} {
        puts stderr "warning: failed to get panel id for '$panel': $err"
        continue
    }

    if {[catch {set rows [get_number_of_rows -id $panel_id]} err]} {
        puts stderr "warning: failed to get row count for '$panel': $err"
        continue
    }

    if {[catch {set cols [get_number_of_columns -id $panel_id]} err]} {
        puts stderr "warning: failed to get column count for '$panel': $err"
        continue
    }

    if {$rows < 2 || $cols < 1} {
        continue
    }

    set header_row 0
    set data_row_start 1

    set headers {}
    for {set c 0} {$c < $cols} {incr c} {
        if {[catch {set h [get_report_panel_data -id $panel_id -row $header_row -col $c]}]} {
            set h "col_$c"
        }
        if {$h eq ""} {
            set h "col_$c"
        }
        lappend headers $h
    }

    set entity_col 0
    for {set c 0} {$c < $cols} {incr c} {
        set h [string tolower [lindex $headers $c]]
        if {[string match "*entity*" $h] || [string match "*hierarchy*" $h] || [string match "*name*" $h]} {
            set entity_col $c
            break
        }
    }

    for {set r $data_row_start} {$r < $rows} {incr r} {
        set entity_label_raw ""
        set entity_path ""
        set entity_indent 0
        set metrics_kv {}

        for {set c 0} {$c < $cols} {incr c} {
            if {[catch {set v [get_report_panel_data -id $panel_id -row $r -col $c]}]} {
                set v ""
            }
            set h [lindex $headers $c]

            if {$c == $entity_col} {
                set entity_label_raw $v
                set entity_indent [leading_spaces_count $v]
                set entity_path [string trim $v]
            }

            lappend metrics_kv [list $h $v]
        }

        if {$entity_path ne ""} {
            lappend entity_rows [list $panel $r $entity_indent $entity_label_raw $entity_path $metrics_kv]
        }
    }
}

if {[catch {set fh [open $out_json w]} err]} {
    puts stderr "failed to open output file '$out_json': $err"
    cleanup
    qexit -error
}

puts $fh "{"
puts $fh {  "matched_panels": [}

for {set i 0} {$i < [llength $matched_panels]} {incr i} {
    set comma ","
    if {$i == [expr {[llength $matched_panels] - 1}]} {
        set comma ""
    }
    puts $fh [format {    "%s"%s} [json_escape [lindex $matched_panels $i]] $comma]
}

puts $fh {  ],}
puts $fh {  "entities": [}

for {set i 0} {$i < [llength $entity_rows]} {incr i} {
    lassign [lindex $entity_rows $i] panel row_index entity_indent entity_label_raw entity_path metrics_kv
    set comma ","
    if {$i == [expr {[llength $entity_rows] - 1}]} {
        set comma ""
    }

    puts $fh "    {"
    puts $fh [format {      "source_panel": "%s",} [json_escape $panel]]
    puts $fh [format {      "row_index": %s,} $row_index]
    puts $fh [format {      "entity_indent": %s,} $entity_indent]
    puts $fh [format {      "entity_label_raw": "%s",} [json_escape $entity_label_raw]]
    puts $fh [format {      "entity_path": "%s",} [json_escape $entity_path]]
    puts $fh "      \"metrics\": {"

    for {set j 0} {$j < [llength $metrics_kv]} {incr j} {
        lassign [lindex $metrics_kv $j] k v
        set comma2 ","
        if {$j == [expr {[llength $metrics_kv] - 1}]} {
            set comma2 ""
        }
        puts $fh [format {        "%s": "%s"%s} [json_escape $k] [json_escape $v] $comma2]
    }

    puts $fh "      }"
    puts $fh [format "    }%s" $comma]
}

puts $fh {  ]}
puts $fh "}"
close $fh

cleanup
qexit -success
