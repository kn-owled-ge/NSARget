/* General initial setup */
clear all
set matsize 10000
cd "C:\Users\finguy\SkyDrive\Documents\Kaniel\"
//cd "D:\SkyDrive\Documents\Kaniel\"
adopath + "C:\Users\finguy\SkyDrive\Documents\Kaniel\Scripts"
//adopath + "D:\SkyDrive\Documents\Kaniel\Scripts"



/* Load in both NSAR-A and NSAR-B datafiles */
insheet using "CRSPFiles\NSARAres.csv", clear
save "CRSPFiles\NSAR.dta", replace
insheet using "CRSPFiles\NSARBres.csv", clear
append using "CRSPFiles\NSAR.dta",gen(ABsrc)
ren v1 	accession
ren v2 	field_cik
ren v3 	signed
ren v4 	series
ren v5 	Q7name
ren v6 	CIK
ren v7 	Adate
ren v8 	Bdate
ren v9 	RegName
ren v10 Q87name
ren v11	Q87cusip
ren v12 Q87ticker
ren v13 Q28A01
ren v14 Q28A02
ren v15 Q28A03
ren v16 Q28A04
ren v17 Q28B01
ren v18 Q28B02
ren v19 Q28B03
ren v20 Q28B04
ren v21 Q28C01
ren v22 Q28C02
ren v23 Q28C03
ren v24 Q28C04
ren v25 Q28D01
ren v26 Q28D02
ren v27 Q28D03
ren v28 Q28D04
ren v29 Q28E01
ren v30 Q28E02
ren v31 Q28E03
ren v32 Q28E04
ren v33 Q28F01
ren v34 Q28F02
ren v35 Q28F03
ren v36 Q28F04
ren v37 Q28G01
ren v38 Q28G02
ren v39 Q28G03
ren v40 Q28G04
ren v41 Q28H00
save "CRSPFiles\NSAR0.dta", replace



/* Tidy data */
use "CRSPFiles\NSAR0.dta", clear
// Find a unique CIK per observation
destring CIK, replace force
drop if field_cik!=CIK & field_cik!=. & CIK!=.
replace CIK=field_cik if CIK==.
drop field_cik accession

// Useless field
drop signed

// Merge date fields
replace Adate="" if Adate=="/  /" | length(Adate)>10
replace Bdate="" if Bdate=="/  /" | length(Bdate)>10
drop if Adate=="" & Bdate==""
replace Adate=Bdate if Adate==""
gen caldt = date(Adate,"MDY", 2030)
drop Adate Bdate ABsrc
drop if cald==.
gen month = mofd(caldt)

// Tidy Q28
drop Q28G* Q28H*  // total and total subject to sales load
drop if Q28A01=="" & Q28A02=="" & Q28A03=="" & Q28A04=="" & Q28F01=="" & Q28F02=="" & Q28F03=="" & Q28F04==""
destring Q28*, replace force
replace Q28A04 = round(Q28A04)
gen Q28 = 0
foreach let in A B C D E F {
	forvalues num = 1/4 {
		replace Q28`let'0`num' = 0 if Q28`let'0`num'==.
		replace Q28 = Q28 + Q28`let'0`num'
	}
}
drop if Q28==0
drop Q28
compress
save "CRSPFiles\NSAR1.dta", replace



/* Make unique identifier per fund */
use "CRSPFiles\NSAR1.dta", clear
// Lacking Q7name is OK as long as there's not more than one fund per CIK-month
bysort CIK month: gen tot=_N
drop if tot>1 & Q7name==""
drop tot
// Get rid of "fund" and "portfolio" at tne of name and then match funds by name
replace Q7name = lower(Q7name)
gen     fund_name = regexs(1) if regexm(Q7name,"^(.*) fund$")
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) fun$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) fu$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) f$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) portfolio$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) portfoli$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) portfol$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) portfo$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) portf$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) port$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) por$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) po$") & fund_name==""
replace fund_name = regexs(1) if regexm(Q7name,"^(.*) p$") & fund_name==""
replace fund_name = Q7name if fund_name==""
drop Q7name
// Gen unique fund ID per fund
sort CIK fund_name month
gen Q28hash = Q28A01 + Q28B01 + Q28C01 + Q28D01 + Q28E01 + Q28F01
by CIK fund_name month: drop if _N==2 & _n==1 & Q28hash==Q28hash[_n+1] & Q28hash!=0
by CIK fund_name month: gen tot=_N
drop if tot>1
drop tot Q28hash
gen FID = .
replace FID=1 if _n==1
replace FID = FID[_n-1] + !(CIK==CIK[_n-1] & fund_name==fund_name[_n-1]) if FID==.
compress
save "CRSPFiles\NSAR2.dta", replace



/* Split into monthly (go wide->long on Q28) */
use "CRSPFiles\NSAR2.dta", clear
foreach let in A B C D E F {
	forvalues num = 1/4 {
		ren Q28`let'0`num' Q28_0`num'`let'
	}
}
reshape long Q28_01 Q28_02 Q28_03 Q28_04, i(FID month) j(letter) string
replace month = month-1 if letter=="E"
replace month = month-2 if letter=="D"
replace month = month-3 if letter=="C"
replace month = month-4 if letter=="B"
replace month = month-5 if letter=="A"

// Lose empty flow obs
drop if Q28_01 == 0 & Q28_02 == 0 & Q28_03 == 0 & Q28_04 == 0

// keep the latest reported repeated month (so the one with the lowest letter)
gsort FID month -letter
by FID month: drop if _N>1 & _n<_N
drop letter

// Tidy series
replace series="" if series=="()"
replace series=subinstr(subinstr(subinstr(subinstr(series,",","",.),")","",.),"(","",.),"'","",.)
replace series=upper(trim(series))
forvalues i=1/16 {
	gen tick`i' = word(series,`i')
}
gsort FID -tick1 -tick2 -tick3 -tick4 -tick5 -tick6 -tick7 -tick8 -tick9 -tick10 -tick11 -tick12 -tick13 -tick14 -tick15 -tick16
local currFID = 1
local new_series = series[1]
forvalues i=1/`=_N' {
	if FID[`i']==`currFID' {
		forvalues j=1/16 {
			if strpos("`new_series'",tick`j'[`i'])==0 {
				local new_series = "`new_series' " + tick`j'[`i']
			}
		}
	}
	else {
		replace series = "`new_series'" if FID==`currFID'
		local currFID=FID[`i']
		local new_series = series[`i']
	}
}
replace series = "`new_series'" if FID==`currFID'
drop tick1-tick16

// Lose fields we are currently not matching on (though they contain data that can help matching!)
drop Q87name Q87cusip Q87ticker caldt RegName fund_name CIK

// Merge Q28_01, _02, _03 -> all are shares sold
gen inflow = Q28_01 + Q28_02 + Q28_03
drop Q28_01 Q28_02 Q28_03
ren Q28_04 outflow
gen flow = inflow-outflow
save "CRSPFiles\NSAR3.dta", replace



/* Prepare for matching algorithm */
use "CRSPFiles\NSAR3.dta", clear
drop outflow inflow
replace flow=. if flow==0
reshape wide flow, i(FID) j(month)
drop flow636-flow639
save "CRSPFiles\NSAR4.dta", replace



/* Prepare CRSP data for matching */
use "CRSPFiles\fund_returns_130507.dta", clear
gen month = mofd(caldt)
drop caldt
gen mtna_p_1=mtna[_n-1] if crsp_fundno==crsp_fundno[_n-1] & month==month[_n-1]+1
merge 1:1 crsp_fundno month using "CRSPFiles\fund info clean.dta"
drop if _merge==2
drop _merge
keep  crsp_fundno mret mtna month mtna_p_1 nasdaq crsp_cl_grp fund_name
replace mtna=. if mtna>.
replace mtna_p_1=. if mtna_p_1>.
replace mret=. if mret>.
// spread the latest crsp_cl_grp to all members of the group by sweeping backward and forward
gsort crsp_fundno -month
replace crsp_cl_grp=crsp_cl_grp[_n-1] if crsp_cl_grp[_n-1]!=. & crsp_fundno==crsp_fundno[_n-1]
sort crsp_fundno month
replace crsp_cl_grp=crsp_cl_grp[_n-1] if crsp_cl_grp[_n-1]!=. & crsp_fundno==crsp_fundno[_n-1]
// give those missing crsp_cl_grp a synthetic one
replace crsp_cl_grp = crsp_fundno + 1000000 if crsp_cl_grp==.
// give every member of the group the total of all mtna in group per month and calc mret
bysort crsp_cl_grp month: egen tot_mtna = total(mtna)
bysort crsp_cl_grp month: egen tot_mtna_p_1 = total(mtna_p_1)
gen mret2 = mret*mtna_p_1
bysort crsp_cl_grp month: egen tot_mret = total(mret2)
replace tot_mret = tot_mret/tot_mtna_p_1
drop mret mret2 mtna mtna_p_1
ren tot_mret mret
ren tot_mtna mtna
ren tot_mtna_p_1 mtna_p_1
save "CRSPFiles\NSAR5a.dta", replace
use "CRSPFiles\NSAR5a.dta", clear
// collect all nasdaq's per fund
drop mtna mtna_p_1 mret fund_name
sort crsp_fundno nasdaq
by crsp_fundno: drop if nasdaq==nasdaq[_n-1] & _n!=1
sort crsp_fundno month
by crsp_fundno: replace nasdaq = nasdaq + " " + nasdaq[_n-1]
by crsp_fundno: keep if _n==_N
// spread nasdaq's to all members of crsp_cl_grp , renaming to series
sort crsp_cl_grp crsp_fundno
by crsp_cl_grp: replace nasdaq = nasdaq + " " + nasdaq[_n-1]
by crsp_cl_grp: keep if _n==_N
drop month crsp_fundno
ren nasdaq series
merge 1:m crsp_cl_grp using "CRSPFiles\NSAR5a.dta", nogen
drop nasdaq fund_name
replace series = itrim(trim(series))
drop if mret<-1 //had a couple obs with weird nums
bysort crsp_cl_grp month: keep if _n==1
// generate flow
gen flow = (mtna-mtna_p_1*(1+mret))*1000
drop if flow==. | flow==0
drop mret mtna mtna_p_1 crsp_fundno
drop if month<366 //July 1990 - lowest month in NSAR data
reshape wide flow, i(crsp_cl_grp) j(month)
append using "CRSPFiles\NSAR4.dta"
save "CRSPFiles\NSAR5.dta", replace



/* Break data into N pieces, run N stata instances to analyze */
local N = 12
local Ntot = 22420
forvalues proc = 1/`N' {
	// Break into pieces
	use "CRSPFiles\NSAR5.dta", clear
	if `proc'<`N' {
		keep if (_n>floor(`Ntot'/`N')*(`proc'-1) & _n<=floor(`Ntot'/`N')*`proc') | _n>`Ntot'
	}
	else {
		keep if (_n>floor(`Ntot'/`N')*(`proc'-1))
	}
	save "CRSPFiles\NSAR5_`proc'.dta", replace
	
	// Save execution file
	file open myfile using NSAR_auto`proc'.do, write replace
	local fw "file write myfile"
	`fw' `"clear all"' _n
	`fw' `"set more off"' _n
	`fw' `"set matsize 10000"' _n
	`fw' `"cd "D:\SkyDrive\Documents\Kaniel\""' _n
	`fw' `"adopath + "D:\SkyDrive\Documents\Kaniel\Scripts""' _n
	`fw' `""' _n
	`fw' `"/* Now do matching!! */"' _n
	`fw' `"local proc = `proc'"' _n
	`fw' `"use "CRSPFiles\NSAR5_\`proc'.dta", clear"' _n	
	`fw' `"count if FID==."' _n
	`fw' `"local Ncrsp = r(N)"' _n
	`fw' `"local MINCNT = 12"' _n
	`fw' `"local WMUL = 20"' _n
	`fw' `"local PER_CORR = 0.1"' _n
	`fw' `"gen match1 = ."' _n
	`fw' `"gen per1 = ."' _n
	`fw' `"gen cnt1 = ."' _n
	`fw' `"gen tic1 = ."' _n
	`fw' `"forvalues i=1/\`Ncrsp' {"' _n
	`fw' `"	qui: gen m_per = 0"' _n
	`fw' `"	qui: gen m_cnt = 0"' _n
	`fw' `"	qui: gen t_cnt = 0"' _n
	`fw' `"	forvalues mt = 366/635 {"' _n
	`fw' `"		qui: if flow\`mt'[\`i'] == . continue"' _n
	`fw' `"		qui: replace m_per = m_per + max(abs((flow\`mt'-flow\`mt'[\`i'])/flow\`mt'[\`i']),abs((flow\`mt'-flow\`mt'[\`i'])/flow\`mt')) if _n>\`Ncrsp' & flow\`mt'!=."' _n
	`fw' `"		qui: replace m_cnt = m_cnt + 1 if _n>\`Ncrsp' & flow\`mt'!=."' _n
	`fw' `"	}"' _n
	`fw' `"	forvalues wrd = 1/42 {"' _n
	`fw' `"		qui: if length(word(series[\`i'],\`wrd')) < 4 continue"' _n
	`fw' `"		qui: replace t_cnt = t_cnt + 1 if _n>\`Ncrsp' & strpos(series, word(series[\`i'],\`wrd'))!=0"' _n
	`fw' `"	}"' _n
	`fw' `"	qui: replace m_per = (m_per/(m_cnt^(1+\`PER_CORR')))/(1+\`WMUL'*t_cnt) if m_cnt>=\`MINCNT'"' _n
	`fw' `"	qui: sum m_per if _n>\`Ncrsp' & m_cnt>=\`MINCNT'"' _n
	`fw' `"	qui: local mn = r(min)"' _n
	`fw' `"	qui: gen ntst = _n if (m_per-\`mn')<1e-8 & _n>\`Ncrsp' & m_cnt>=\`MINCNT'"' _n
	`fw' `"	qui: sum ntst"' _n
	`fw' `"	qui: local qtst = r(min)"' _n
	`fw' `"	if "\`qtst'"!="." {"' _n
	`fw' `"		qui: replace match1 = FID[\`qtst'] if _n==\`i'"' _n
	`fw' `"		qui: replace per1 = m_per[\`qtst'] if _n==\`i'"' _n
	`fw' `"		qui: replace cnt1 = m_cnt[\`qtst'] if _n==\`i'"' _n
	`fw' `"		qui: replace tic1 = t_cnt[\`qtst'] if _n==\`i'"' _n
	`fw' `"	}"' _n
	`fw' `"	qui: drop ntst m_per m_cnt t_cnt"' _n
	`fw' `"	disp \`i'"' _n
	`fw' `"}"' _n
	`fw' `"save "CRSPFiles\NSAR6_\`proc'.dta", replace"' _n
	file close myfile

	winexec cmd /c "C:\Progra~2\Stata12\StataSE-64.exe do D:\SkyDrive\Documents\Kaniel\NSAR_auto`proc'.do"
}	


/********************************/
/* WAIT FOR EXECUTION TO FINISH */
/********************************/



/* Collect pieces */
local N = 12
local Ntot = 22420
use "CRSPFiles\NSAR6_1.dta", clear
forvalues proc = 2/`N' {
	drop if (_n>floor(`Ntot'/`N')*(`proc'-1))
	append using "CRSPFiles\NSAR6_`proc'.dta"
}
drop if (_n>`Ntot')
// Drop funds which have no post-1999 obs
egen tmp = rowtotal(flow468-flow635), missing
drop if tmp==.
drop tmp flow*
// Cleanup
replace FID=match1
drop if FID==.
drop match1
// Check how many funds got matched too much
bysort FID: gen tot=_N
sum tot
drop tot
save "CRSPFiles\NSAR6.dta", replace



/* Prepare datasets to be merged using NSAR6 and merge */
// CRSP
use "CRSPFiles\NSAR5.dta", clear
drop if FID!=.
save "CRSPFiles\NSAR5b.dta", replace
use "CRSPFiles\NSAR5a.dta", clear
keep crsp_fundno crsp_cl_grp month fund_name 
drop if month<468
bysort crsp_fundno fund_name: keep if _n==1
drop if fund_name==""
sort crsp_fundno month
by crsp_fundno: keep if _n==_N
drop month
merge m:1 crsp_cl_grp using "CRSPFiles\NSAR5b.dta"
keep  crsp_fundno fund_name crsp_cl_grp series
ren fund_name fund_name_crsp
ren series series_crsp
save "CRSPFiles\NSAR6_crsp.dta", replace
// NSAR
use "CRSPFiles\NSAR2.dta", clear
keep RegName fund_name month FID series
drop if month<468
bysort FID fund_name: keep if _n==1
by FID: drop if fund_name=="" & _N>1
sort FID month
by FID: keep if _n==_N
drop month
gen fund_name_nsar = RegName + " ## " + fund_name
ren series series_nsar
drop RegName fund_name
save "CRSPFiles\NSAR6_nsar.dta", replace
// Merge
use "CRSPFiles\NSAR6.dta", clear
drop series
merge 1:m crsp_cl_grp using "CRSPFiles\NSAR6_crsp.dta"
keep if _merge==3
drop _merge
merge m:1 FID using "CRSPFiles\NSAR6_nsar.dta"
keep if _merge==3
drop _merge
replace per1 = round(per1*10000)
gsort per1 -tic1 -cnt1
//drop series_crsp series_nsar
order fund_name_crsp fund_name_nsar, last
save "CRSPFiles\NSAR7.dta", replace
gen matched=1
outsheet using "CRSPFiles\NSAR7.csv", comma replace



/***********************************/
/* DO MANUAL VERIFICATION OF MATCH */
/***********************************/



// Read back matched dataset
insheet using "CRSPFiles\NSAR7_edit.csv", comma clear
qui: count if matched==1
local match = r(N)
qui: count
local tot = r(N)
disp (`match'/`tot')
drop if matched==0
keep fid crsp_fundno
ren fid FID
drop if crsp_fundno == .
save "CRSPFiles\NSAR8.dta", replace



// Delete all intermediate step dta files
