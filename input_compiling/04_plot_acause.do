        
        /*
        
        stata-mp  "/home/j/temp/sadatnfs/dex_decomp_chunk/acause.dta"

        set more off

        levelsof acause, local(acauses)

        foreach a in `acauses' {
            !qsub   -N "graph_`a'"  -o /share/temp/sgeoutput/sadatnfs/output -e /dev/null -pe multi_slot 6 /ihme/code/dex/madcampb/dex/10_resources/13_other/stata_shell.sh /home/j/temp/sadatnfs/dex_decomp_chunk/04_plot_acause.do "`a'"
        }


        */


        ** **************************************************************************
        ** Prepare STATA for use
        ** **************************************************************************
        // Define J drive (data) for cluster (UNIX) and Windows (Windows)
                if c(os) == "Unix" {
                        global j "/home/j"
                        set odbcmgr unixodbc
                }
                else if c(os) == "Windows" {
                        global j "J:"
                      x
                 }

        //  Set up run-time environment
                clear all
                set more off
                capture restore, not
                set maxvar 32000

        // args
        local a "`2'"

        ** local a "_ntd"

        import delimited using  "$j/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/`a'.csv",  clear

        drop *_med *_mad *_z

        ** First diff the data
        sort function age sex acause draw year
        egen groups = group(function age sex acause draw)
        xtset groups year

        foreach var of varlist expend volume cases {
                gen fd_`var' = D.`var'
        }



        egen exp_med = median(fd_expend), by(function age sex acause  )
        egen vol_med = median(fd_volume), by(function age sex acause  )
        egen case_med = median(fd_cases), by(function age sex acause  )

        egen exp_mad = mad(fd_expend), by(function age sex acause  )
        egen vol_mad = mad(fd_volume), by(function age sex acause  )
        egen case_mad = mad(fd_cases), by(function age sex acause  )

        gen exp_z =((fd_expend - exp_med * .6745)/exp_mad)
        gen vol_z =((fd_volume - vol_med * .6745)/vol_mad)
        gen case_z =((fd_cases - case_med * .6745)/case_mad)


        keep function acause year age sex draw exp_z vol_z case_z

        do "$j/Usable/Tools/ADO/pdfmaker_Acrobat11.do"
                
        pdfstart using "$j/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/`a'_expend.pdf"

        foreach fn in "AM" "DV" "ER" "IP" "LT" "RX" {
                hist exp_z if function == "`fn'", name(exp_`fn', replace) title("`fn'") 
        }

        gr combine exp_AM exp_DV exp_ER exp_IP exp_LT exp_RX, title("`a'")
        pdfappend

        pdffinish



        pdfstart using "$j/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/`a'_volume.pdf"

        foreach fn in "AM" "DV" "ER" "IP" "LT" "RX" {
                hist vol_z if function == "`fn'", name(vol_`fn', replace) title("`fn'") 
        }

        gr combine vol_AM vol_DV vol_ER vol_IP vol_LT vol_RX, title("`a'")
        pdfappend

        pdffinish




        pdfstart using "$j/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/`a'_cases.pdf"

        foreach fn in "AM" "DV" "ER" "IP" "LT" "RX" {
                hist case_z if function == "`fn'", name(cases_`fn', replace) title("`fn'") 
        }

        gr combine cases_AM cases_DV cases_ER cases_IP cases_LT cases_RX, , title("`a'")
        pdfappend

        pdffinish


        save "$j/Project/IRH/DEX/USA/10_resources/13_other/data/acauses/z_scored/`a'.dta", replace


