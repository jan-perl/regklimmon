# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
#werkboek met voortgang maatregelen klimaat monitor
# -

import pandas as pd
import numpy as np
import subprocess
import requests
import json
import os
import io
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

getgeop=False
if getgeop:
    os.system("pip install geopandas")
    os.system("pip install contextily")

import geopandas
import contextily as cx
import xyzservices.providers as xyz
import matplotlib.pyplot as plt

# +
# Naam: Jan Hoogenraad
#Organisatie: Spoorgloren
#Mail: jan.hoogenraad@spoorgloren.nl
# voorbeelden van https://klimaatmonitor.databank.nl/content/handleiding-open-data-service
# -

myapikey = subprocess.getoutput("cat ../data/rkm-api.key")
print(myapikey)
baseurl='https://klimaatmonitor.databank.nl/jiveservices/odata'

url = baseurl+'/Variables' 
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r)

if r:
    print("Success!")
else:
    raise Exception(f"Non-success status code: {r.status_code}")

rdf1=pd.read_json(r.content)
rdf1

url = baseurl+'/GeoLevels' 
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
vpd= pd.read_json(r.content)['value']
vpd

pval=pd.json_normalize(vpd)
pval

# +
#definieer cache, zodat
if not 'kkmocache' in globals():
    print ("init kkmocache")
    kkmocache= {}
def getkkmo(src):
#    print (kkmocache)
    if src in kkmocache.keys():
#        print ("from cache")        
        rv= kkmocache[src] 
    else:
        nextlinkstr='@odata.nextLink'    
        url = baseurl+src
        headers = {'apikey': myapikey}
        getblk=True
        pval=[]
        while getblk:
            r = requests.get(url, headers=headers)
            if not r:
                raise Exception(f"Non-success status code: {r.status_code}")
            blkcont= pd.read_json(r.content)
    #        print (blkcont.columns)
            vpd= blkcont['value']    
            bval=pd.json_normalize(vpd)
            bval=bval.drop(['@odata.type','@odata.id'], axis=1)
            getblk= nextlinkstr in blkcont.columns
            if getblk:
    #            bval=bval.drop(['@odata.nextLink'], axis=1)
                url=blkcont['@odata.nextLink'] [0] 
                print('getting next block '+url) 
            pval.append(bval)
    #    print(pval)        
            rv= pd.concat(pval)
            kkmocache[src]=rv #.to_dict()
            rv= kkmocache[src]
#            rv= pd.DataFrame.from_dict(kkmocache[src])
#            print (kkmocache)
    return rv

r=getkkmo('/GeoLevels')
r
# -

#haal namen RES regios op
GeoLevels_res =getkkmo("/GeoLevels('res')/GeoItems")
u16res='res_14'
GeoLevels_res

# +
#gedefinieerde doelen sets

doeljaarset_2030=[ 'Flevoland','Zeewolde',
              'Stadskanaal', 'Borger-Odoorn'  ,   'Veere' ,   
                                'Goeree-Overflakkee','Wijk bij Duurstede']
#nooit op 1 of 0 zetten ivm sorteren in Y
doelbesp_99 = [ 'Flevoland','Zeewolde','Noord-Holland Noord',
            'Ommen',"Noordoostpolder" ,"Het Hogeland" ,"Ameland", "Schiermonnikoog",
            'Steenbergen', 'Haarlemmermeer','Son en Breugel' , 
            'Brummen','Duiven','Veere', 'Vlissingen' ,'Woensdrecht' ,
            'Goeree-Overflakkee']

#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
doeljaar_gem_aanp=pd.read_excel('../data/doeljaar_gem_aanp.xlsx')
doeljaar_gem_aanp=doeljaar_gem_aanp[['GeoLevel','Name','Jaar','Besparing']] 
#print (doeljaar_gem_aanp[doeljaar_gem_aanp['Jaar']==2050])
doeljaarset_2050=list(doeljaar_gem_aanp[doeljaar_gem_aanp['Jaar']==2050]['Name'])
doeljaarset_2050

# +
some_string="""regsel,lev,minx,maxx,miny,maxy,sscale,gscale
u16,gemeente,119000, 154000 , 438000 ,471000,0.5,1 
randst,gemeente,10000, 129000, 398000 , 495000,0.1,1  
nw,gemeente,10000 , 200000, 490000 , 700000,0.1,1  
no,gemeente,170000, 300000, 490000 ,700000,0.1,1 
mo,gemeente,149000, 300000 , 398000,491000,0.1,1 
zo,gemeente,119000, 300000 , 190000 ,400000,0.1,1 
zw,gemeente,10000 , 130000 , 190000 ,400000,0.1,1
agem,gemeente,100   , 300000,100,700000,.1,1
resregs,res,100   , 300000,100,700000,0.02,5 
provs,provincie,100, 300000,100,700000,0.02,5
nl,nederland,100   , 300000,100,700000,.002,1
"""
    #read CSV string into pandas DataFrame    
regiosels_df= pd.read_csv(io.StringIO(some_string), sep=",").set_index('regsel')

glb_regsel='u16'
#glb_regsel='resregs'
my_regsel=glb_regsel

def make_selbox(my_regsel):
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    GeoLevels_reg =getkkmo("/GeoLevels('"+param_regiosels['lev']+"')/GeoItems")
    GeoLevels_reg['GeoLevel'] =param_regiosels['lev']
    s1= GeoLevels_reg [(GeoLevels_reg['PointX']> param_regiosels['minx'] ) &
                                    (GeoLevels_reg['PointX']< param_regiosels['maxx'] ) &
                                    (GeoLevels_reg['PointY']> param_regiosels['miny'] ) &
                                    (GeoLevels_reg['PointY']< param_regiosels['maxy'] ) ]
    if (my_regsel=='agem'):
        s1=s1[s1['Name'].isin(doeljaarset_2030+doeljaarset_2050+doelbesp_99)]
    return s1
GeoLevels_gem_selbox = make_selbox(glb_regsel)

GeoLevels_gem_selbox
# -



print (getkkmo('/PeriodLevels'))

#alleen eerste 1000 records getoond
getkkmo('/Variables').to_excel('../intermediate/rkm-variables.xlsx')

#print (getkkmo('/DataSources'))
getkkmo('/DataSources').to_excel('../intermediate/rkm-datasources.xlsx')

# +
# werkt niet als 21 record terug komt print (getkkmo("/DataSources('cbsapi_83140ned')"))
# -

url = baseurl+"/DataSources('cbsapi_83140ned')"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))
#vpd= pd.read_json(r.content.decode('utf-8'))['value']
#print (vpd)

url = baseurl+"/Variables('bevtot')/GeoLevels"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))

url = baseurl+"/Variables('zonpvtj')/GeoLevels"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))

print (getkkmo("/Variables('zonpv_twh_groot_res')/GeoLevels"))

# +
url = baseurl+"/Variables('cbsapi_70072ned')/GeoLevels('gemeente')/PeriodLevels('YEAR')/Periods('2016')/Values"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))

#print (getkkmo("/Variables('cbs_zon')/GeoLevels('gemeente')/PeriodLevels('YEAR')/Periods('2016')/Values"))


# +
#nu eens energie per RES regio ophalen
# -

baseyear='2012'
(getkkmo("/Variables('energie_totaal_combi')/GeoLevels('res')/PeriodLevels('year')/Periods('"+
                baseyear+"')/Values"))

baseyear='2010'
(getkkmo("/Variables('energie_totaal_combi')/GeoLevels('provincie')/PeriodLevels('year')/Periods('"+
                baseyear+"')/Values"))

print (getkkmo("/Variables('hern_tot')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values"))

totenjaarres=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
totenjaarres['VarName']='energie_totaal_combi'
print (totenjaarres[totenjaarres['ExternalCode']==u16res])

hernjaarres=getkkmo("/Variables('hern_tot')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
hernjaarres['VarName']='hern_tot'
print (hernjaarres[hernjaarres['ExternalCode']==u16res])

#deze is in GWh
#bij deze kunnen ook ook gemeentes worden opgevraagd
#print (getkkmo("/Variables('res_bod')/GeoLevels"))
rbod1=getkkmo("/Variables('res_bod')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
bodresjr= rbod1[rbod1['ValueString']!='?'].copy()
bodresjr['ValueString']= pd.to_numeric(bodresjr['ValueString'],errors='coerce')*3.6
bodresjr['VarName']='RES bod'
bodresjr

print (getkkmo("/Variables('res_pijplijn')/GeoLevels"))

print (getkkmo("/Variables('eml_kb_aant_inr')/GeoLevels"))

hernaand=pd.concat([totenjaarres, hernjaarres,bodresjr],copy=False)
hernaand['Jaar']= pd.to_numeric(hernaand['Period'],errors='coerce') 
hernaand['Value']= pd.to_numeric(hernaand['ValueString'],errors='coerce')
print(hernaand)

hernaand_u16=hernaand[hernaand['ExternalCode']==u16res]
sns.lineplot(x='Jaar',y="Value",hue="VarName",data=hernaand_u16)
sns.scatterplot(x='Jaar',y="Value",hue="VarName",data=hernaand_u16)
plt.ylabel('Energie [TJ]')
plt.title('Totaal energieverbruik en hernieuwbare energie U16')

# +
#fijn, nu per gemeente
# -

#gemeentebasis data
glb_refyear='2010'
def mkselboxref(gem_selbox,refyear):
    thislev=gem_selbox.iloc[0]['GeoLevel']
#    print(thislev)
    refentot=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('"+thislev+"')/PeriodLevels('year')/Periods('"+
                   refyear+"')/Values")
    ref_selbox= refentot.merge( gem_selbox, how='left',                
                                     on=['ExternalCode','GeoLevel'])
    ref_selbox= ref_selbox[ref_selbox['Name'].isna()==False]
    ref_selbox['refwrd']=pd.to_numeric(ref_selbox['ValueString'],errors='coerce')
    ref_selbox['refperiod']=pd.to_numeric(ref_selbox['Period'],errors='coerce')
    if(pd.isna(ref_selbox['refwrd']).any()):
        print ("Using nearest years for reference")
        refentot=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('"+thislev+"')/PeriodLevels('year')/Periods('all')/Values")
        ref_selbox= refentot.merge( gem_selbox, how='left',                
                                     on=['ExternalCode','GeoLevel'])
        ref_selbox['refwrd']=pd.to_numeric(ref_selbox['ValueString'],errors='coerce')
        ref_selbox= ref_selbox[ref_selbox['Name'].isna()==False].copy()
        ref_selbox= ref_selbox[ref_selbox['refwrd'].isna()==False].copy()
        ref_selbox['refperiod']=pd.to_numeric(ref_selbox['Period'],errors='coerce')
        ref_selbox["diffyr"] = abs( ref_selbox['refperiod']-pd.to_numeric(refyear) )                                  
        ref_selbox=ref_selbox.sort_values("diffyr").groupby('ExternalCode').\
             agg('first').reset_index().drop(['diffyr'],axis=1)
        
    ref_selbox=ref_selbox.drop(['Period','ValueString','Description'], axis=1)
    return ref_selbox
refresreg_selbox=  mkselboxref(make_selbox('resregs'),glb_refyear)      
(refgem_selbox) 

refgem_selbox=  mkselboxref(GeoLevels_gem_selbox,glb_refyear)  
refgem_selbox

#providers = cx.providers.flatten()
#providers
prov0=cx.providers.nlmaps.grijs.copy()
#print( cbspc4data.crs)
print (prov0)
plot_crs=3857
#plot_crs=28992
if 1==1:
#    prov0['url']='https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/{variant}/EPSG:28992/{z}/{x}/{y}.png'
    prov0['url']='https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/{variant}/EPSG:3857/{z}/{x}/{y}.png'    
#    prov0['bounds']=  [[48.040502, -1.657292 ],[56.110590 ,12.431727 ]]  
    prov0['bounds']=  [[48.040502, -1.657292 ],[56.110590 ,12.431727 ]]  
    prov0['min_zoom']= 0
    prov0['max_zoom'] =12
    print (prov0)

#check dat grootte als oppervalk schaalt: links 2^2 groter oppervlak rechts 2* groter oppervlak
siztst =pd.DataFrame( {'PointX': [0,0,100,100 ],
        'PointY': [0,0,0,0],
        'refwrd' : [2000,8000,2000,4000]} )
print(siztst)
plt.scatter(siztst['PointX'],siztst['PointY'],siztst['refwrd'],alpha=.1) 


# +
#en nu netjes, met schaal in km
def plaxkm(x, pos=None):
      return '%.0f'%(x/1000.)
    
def selboxbasusg(ref_selbox,my_regsel):
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    print (param_regiosels)
    thislev=ref_selbox.iloc[0]['GeoLevel']
#    print(thislev)
    svals= ref_selbox['refwrd']*param_regiosels['sscale']
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.scatter(ref_selbox['PointX'],ref_selbox['PointY'],s=svals,alpha=.1) 
    for idx, row in pd.DataFrame(ref_selbox).iterrows():        
        plt.text(row['PointX'],row['PointY'],
                 row['Name']+'\n'+(str(row['refwrd'])),
                 ha='right',va='center') 
    #cx.add_basemap(ax, source= prov0)
    plt.title('Totaal energieverbruik 2010 per '+thislev)
    plt.gca().set_aspect('equal')
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    return fig
r=selboxbasusg(refgem_selbox,glb_regsel)
# -

refgem_selbox_df = pd.DataFrame(refgem_selbox)
gdf = geopandas.GeoDataFrame(
    refgem_selbox_df, geometry=geopandas.points_from_xy(refgem_selbox_df.PointX, refgem_selbox_df.PointY), 
      crs="epsg:28992"
)
print(gdf)

#REs bod per gemeente: er worden wel records getoond, maar deze bevatten geen getallen
resbodgem_selbox_o= getvalselbox ('RES bod',
    "/Variables('res_bod')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('2030')/Values",
                                   False)
#resbodgem_selbox= resbodgem_selbox[ False== resbodgem_selbox['Besparing'] .isna()].copy()
#resbodgem_selbox['Besparing']=  resbodgem_selbox['Besparing']*3.6
resbodgem_selbox_o

#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
#    
resbodgem_selbox =pd.read_excel('../data/resbod_gem_aanp.xlsx')


# +
doeljaar_gem =resbodgem_selbox_o.copy()
doeljaar_gem ['VarName'] = 'ENeutr Doel'
doeljaar_gem ['Jaar'] = 2040
doeljaar_gem ['Jaar'] = doeljaar_gem ['Jaar'].where (
    doeljaar_gem ['Name'].isin(doeljaarset_2030) == False, 2030 ).where (
    doeljaar_gem ['Name'].isin(doeljaarset_2050) == False, 2050 )
#nooit op 1 of 0 zetten ivm sorteren in Y
doeljaar_gem ['Besparing'] =0.99
doeljaar_gem ['Besparing'] = doeljaar_gem ['Besparing'].where (
    doeljaar_gem ['Name'].isin(doelbesp_99), 0.5 )

doeljaar_gem.to_excel('../intermediate/doeljaar_gem_auto.xlsx')
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
#if glb_regsel=='u16':
#    doeljaar_gem=pd.read_excel('../data/doeljaar_gem_aanp.xlsx')
doeljaar_gem['Target']=doeljaar_gem['Besparing']
sdbb=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbb['Jaar']=int (glb_refyear)
sdbb['Target']=0
sdbg=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbg['Jaar']=int (glb_refyear)
sdbg['Target']=1


doeljaar_gem

# +
#refgem_selbox_df.svals= pd.to_numeric(refgem_selbox_df.ValueString)*.3
#fig, ax = plt.subplots(figsize=(6, 4))
#pdf=refgem_selbox_df[['svals']]
#print(pdf)
#pdf.plot()
#plt.scatter(refgem_selbox_df.PointX,refgem_selbox_df.PointY,s=refgem_selbox_df.svals,alpha=.1) 
#cx.add_basemap(ax, source= prov0)

# +
#Nu relatieve waarden

# +
selboxmergeon=['ExternalCode', 'GeoLevel','PeriodLevel']
def getvalselbox(colnm,query,decr):
    thisdat=getkkmo(query)
    thisdat['VarName']=colnm
    thisdat_selbox= thisdat.merge( refgem_selbox, how='left', on=selboxmergeon )
    thisdat_selbox= thisdat_selbox[thisdat_selbox['Name'].isna()==False].copy()
    thisdat_selbox['Jaar']= pd.to_numeric(thisdat_selbox['Period'],errors='coerce') 
    thisdat_selbox['Waarde']=   pd.to_numeric(thisdat_selbox['ValueString'],errors='coerce')
    return (thisdat_selbox)

eml_kb_aant_inr_selbox= getvalselbox ('eml_kb_aant_inr',
    "/Variables('eml_kb_aant_inr')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
(eml_kb_aant_inr_selbox)
# -

pltdat=eml_kb_aant_inr_selbox
sns.lineplot(x='Jaar',y="Waarde",hue="Name",data=pltdat)
plt.ylabel('Fractie energiebesparing')
plt.title('Energiebesparing U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)


#https://www.rvo.nl/onderwerpen/toelichting-rubriceringscode-informatieplicht
#1: hoogste prioriteit bij toezicht: minstens één erkende maatregel de antwoordoptie ‘(Nog) niet uitgevoerd’ 
#2: 2e prioriteit bij toezicht :  minstens één erkende maatregel is de antwoordoptie ‘Gedeeltelijk uitgevoerd’ selecteerde. U selecteerde voor geen enkele maatregel de antwoordoptie ‘(Nog) niet uitgevoerd’.
#3: nadere beoordeling : informatieplichtrapportage is een verplichte bijlage toegevoegd waarin u onderbouwt dat alle energiebesparende maatregelen met een terugverdientijd van 5 jaar of minder zijn uitgevoerd.
#4: in principe voldaan : voor alle toepasselijke erkende maatregelen de antwoordoptie ‘Volledig uitgevoerd’ of ‘Alternatief uitgevoerd’ selecteerde
#5: rapportage van energiegebruik onder de grens: vrijwillig rapporteert omdat het jaarlijks energiegebruik van de locatie lager is dan 50.000 kWh elektriciteit én lager is dan 25.000 m3 aardgasequivalent.
def detvarseq (basevar,rlevsel,levvar):
    olst=[]
    ilst=[]
    for rlev in rlevsel:
        rlet=str(rlev)
        t= getvalselbox (basevar+rlet,
        "/Variables('"+basevar+rlet+"')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                       False)
        t[levvar]=rlev
        ilst.append(t)
        slst=pd.concat(ilst)
        sv2= slst[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
        sv2=sv2.rename(columns={"Waarde":"SomWaarde" })
        t=t.merge(sv2,on=["Name",'Jaar'],how='left')               
        olst.append(t)
    return (pd.concat(olst))[::-1]
eml_kb_perc_rx_selbox=detvarseq ('eml_kb_perc_r',[3,2,1],'Rubriceringscode')
if False:    
    eml_kb_perc_rx_selbox45= eml_kb_perc_rx_selbox[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
    eml_kb_perc_rx_selbox45['Rubriceringscode']= 45
    eml_kb_perc_rx_selbox45['Waarde']= 100- eml_kb_perc_rx_selbox45['Waarde']
    eml_kb_perc_rx_selbox=pd.concat(olst+[eml_kb_perc_rx_selbox45])

selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="Rubriceringscode",
                 data=eml_kb_perc_rx_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
plt.ylabel('Percentage bedrijven')
plt.title('EML rapportage rubricering')
g.add_legend()
figname = "../output/perc_rx_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

pwonlab_selbox=detvarseq ('pwonlab',['g','f','e','d','c','b','a'],'pwonlab')



selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="pwonlab",
                 data=pwonlab_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
plt.ylabel('Percentage woningen')
plt.title('Woninglabels per gemeente')
g.add_legend()
figname = "../output/pwonlab_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

rlevselg=['g','f','e','d','c','b','a','aa','aaa','a4','a5']
rlevselg=['g','f','e','d','c','b','atot']
geblab_selbox=detvarseq ('geblab',rlevselg,'geblab')

#dit moet nog genomeerd op aantallen BAG adressen
selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="geblab",
                 data=geblab_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/geblab_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

geblabac = geblab_selbox[geblab_selbox['geblab'].isin(['d','atot','a5'])]
geblabac = geblabac[geblabac['Jaar']==2023]
geblabac= geblabac.pivot(columns='geblab',index='Name',values="SomWaarde").reset_index()
geblabac

#pd.read_dbf("../data/pergemeented_v20240301_v2.dbf")
thisamax='atot'
EPcheck_ref = geopandas.read_file("../data/pergemeented_v20240301_v2.dbf")
EPcheck_ref['Ntolabel'] = EPcheck_ref['N_NOK']+ EPcheck_ref['N_OK'] + EPcheck_ref['N_uitz']+ EPcheck_ref['N_geen']
EPcheck_chk = geblabac.merge(EPcheck_ref,how='left',left_on="Name",right_on="gemeente")
#EPcheck_chk[['Name','a5','d','N_geen','N_OK','N_NOK']]
EPcheck_chksel = EPcheck_chk[['Name',thisamax,'d','N_geen','N_OK','N_NOK',"N_niett","N_uitz","Ntolabel"]].copy()
EPcheck_chksel['atotdiff']= EPcheck_chksel[thisamax] -EPcheck_chksel['N_NOK']- EPcheck_chksel['N_OK']
EPcheck_chksel['uitzrat'] = EPcheck_chksel['N_uitz'] / EPcheck_chksel['atotdiff'] 
EPcheck_chksel['ddiff'] = EPcheck_chksel['d'] - EPcheck_chksel['N_NOK']
EPcheck_chksel
#EPcheck_chksel.sum()

EPcheck_ref_selcols = EPcheck_ref[['gemeente','Ntolabel']]
geblaltotal = geblab_selbox[geblab_selbox['geblab'].isin(['atot','a5'])].copy()
geblaltotalc = geblaltotal[['Name','Jaar','SomWaarde']].copy()
geblaltotalc = geblaltotalc.rename({'SomWaarde':"SomMaxa"},axis=1)
#print(geblaltotalc)
geblaltotal['geblab']="ongelabeld"
geblaltotal['SomWaarde']=0
geblab_selboxe= pd.concat([geblab_selbox,geblaltotal])
geblabr_selbox = geblab_selboxe.merge(EPcheck_ref_selcols,how='left',left_on="Name",right_on="gemeente"). merge(geblaltotalc,
       how='left',on=["Name","Jaar"])
geblabr_selbox[ 'Ntolabel'] = geblabr_selbox[ 'Ntolabel'] . where (geblabr_selbox[ 'Ntolabel'] >  geblabr_selbox[ 'SomMaxa'], geblabr_selbox[ 'SomMaxa'])
geblabr_selbox[ 'SomWaarde'] = 100*(geblabr_selbox[ 'SomWaarde'] + geblabr_selbox[ 'Ntolabel'] - geblabr_selbox[ 'SomMaxa'] ) / geblabr_selbox[ 'Ntolabel'] 
geblabr_selbox

selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="geblab",
                 data=geblabr_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/geblabr_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")


# +
#geo plots
# -

def pltemlkbngeo(ref_selbox,my_regsel, datah, datab,doeljaar): 
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    fig=selboxbasusg(ref_selbox,glb_regsel)
    
    yrmtr=100
    rmtr=5000/100
    ctryr=2030
    xdat=(datab['Jaar']-ctryr) * yrmtr +datab['PointX']
    ydat=(datab['SomWaarde']-50) * rmtr +datab['PointY']
    plt.scatter(xdat,ydat,label="Code 2 of 3 ")
    xdat=(datah['Jaar']-ctryr) * yrmtr +datah['PointX']
    ydat=(datah['SomWaarde']-50) * rmtr +datah['PointY']
    plt.scatter(xdat,ydat,label="Code 3")
    sdbk=doeljaar_gem.drop(['Besparing'], axis=1).copy()
    sdbk['Jaar']=np.nan
    sdbk['Target']=-1
    datad=pd.concat([sdbb,doeljaar_gem,sdbk],copy=False).sort_values(['Name','Target'])
    xdat=(datad['Jaar']-ctryr) * yrmtr +datad['PointX']
    ydat=(datad['Target']*2-0.5) *100* rmtr +datad['PointY']
    plt.plot(xdat,ydat,label="Mogelijk pad")
    #sns.scatterplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.title('Gerapporteerde EML rubricering bedrijven vanaf 2010')
    figname = "../output/eml_kb_perc_rx_geo_"+my_regsel+'.png';
    fig.savefig(figname,dpi=300) 
pltemlkbngeo(refgem_selbox,glb_regsel,
            eml_kb_perc_rx_selbox[eml_kb_perc_rx_selbox["Rubriceringscode"]==3],
            eml_kb_perc_rx_selbox[eml_kb_perc_rx_selbox["Rubriceringscode"]==2],
            doeljaar_gem)    


def pltpwonlabgeo(ref_selbox,my_regsel, datah, datab,doeljaar): 
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    fig=selboxbasusg(ref_selbox,glb_regsel)
   

    yrmtr=100
    rmtr=5000/100
    ctryr=2030
    xdat=(datab['Jaar']-ctryr) * yrmtr +datab['PointX']
    ydat=(datab['SomWaarde']-50) * rmtr +datab['PointY']
    plt.scatter(xdat,ydat,label="max Label D")
    xdat=(datah['Jaar']-ctryr) * yrmtr +datah['PointX']
    ydat=(datah['SomWaarde']-50) * rmtr +datah['PointY']
    plt.scatter(xdat,ydat,label="max label B")
    sdbk=doeljaar_gem.drop(['Besparing'], axis=1).copy()
    sdbk['Jaar']=np.nan
    sdbk['Target']=-1
    datad=pd.concat([doeljaar_gem,sdbg,sdbk],copy=False).sort_values(['Name','Target'])
    #print ((datad['Target']*2-1) )
    xdat=(datad['Jaar']-ctryr) * yrmtr +datad['PointX']
    ydat=(datad['Target']*2-1-0.5) *100* rmtr +datad['PointY']
    plt.plot(xdat,ydat,label="Mogelijk pad")
    #sns.scatterplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.title('Gerapporteerde Energielabels woningen  vanaf 2010')
    figname = "../output/pwonlab_geo_"+my_regsel+'.png';
    fig.savefig(figname,dpi=300) 
pltpwonlabgeo(refgem_selbox,glb_regsel,
    pwonlab_selbox[pwonlab_selbox["pwonlab"]=="b"],
    pwonlab_selbox[pwonlab_selbox["pwonlab"]=="d"] ,            
            doeljaar_gem)        


def pltgeblabrgeo(ref_selbox,my_regsel, datah, datab,doeljaar): 
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    fig=selboxbasusg(ref_selbox,glb_regsel)
   

    yrmtr=100
    rmtr=5000/100
    ctryr=2030
    xdat=(datab['Jaar']-ctryr) * yrmtr +datab['PointX']
    ydat=(datab['SomWaarde']-50) * rmtr +datab['PointY']
    plt.scatter(xdat,ydat,label="ongelabeld")
    xdat=(datah['Jaar']-ctryr) * yrmtr +datah['PointX']
    ydat=(datah['SomWaarde']-50) * rmtr +datah['PointY']
    plt.scatter(xdat,ydat,label="max Label B")
    sdbk=doeljaar_gem.drop(['Besparing'], axis=1).copy()
    sdbk['Jaar']=np.nan
    sdbk['Target']=-1
    datad=pd.concat([doeljaar_gem,sdbg,sdbk],copy=False).sort_values(['Name','Target'])
    #print ((datad['Target']*2-1) )
    xdat=(datad['Jaar']-ctryr) * yrmtr +datad['PointX']
    ydat=(datad['Target']*2-1-0.5) *100* rmtr +datad['PointY']
    plt.plot(xdat,ydat,label="Mogelijk pad")
    #sns.scatterplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.title('Gerapporteerde Energielabels utiliteit  vanaf 2010')
    figname = "../output/pwonlab_geo_"+my_regsel+'.png';
    fig.savefig(figname,dpi=300) 
pltgeblabrgeo(refgem_selbox,glb_regsel,
    geblabr_selbox[geblabr_selbox["geblab"]=="b"],
    geblabr_selbox[geblabr_selbox["geblab"]=="ongelabeld"] ,            
            doeljaar_gem)


