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
#werkboek met energie opbrengsten regeionale klimaat monitor
#zet glb_regsel op de gewenste doorsnede
# -

import pandas as pd
import numpy as np
import subprocess
import requests
import json
import os
import io
import re
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
#definieerde doelen sets

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
doeljaar_gem_aanpo=pd.read_excel('../data/doeljaar_gem_aanp.xlsx')
doeljaar_gem_aanp=doeljaar_gem_aanpo[['GeoLevel','Name','Jaar','Besparing']] 
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
make_selbox('resregs')




print (getkkmo('/PeriodLevels'))

#alleen eerste 1000 records getoond
getkkmo('/Variables').to_excel('../intermediate/rkm-variables.xlsx')

#print (getkkmo('/DataSources'))
getkkmo('/DataSources').to_excel('../intermediate/rkm-datasources.xlsx')

# +
# werkt niet als 21 record terug komt print (getkkmo("/DataSources('cbsapi_83140ned')"))

# +
#deze stukken code zijn om ophalen data te debuggen
#geen functie in nofmrale workflow
# -

url = baseurl+"/DataSources('cbsapi_83140ned')"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
rp = r.content.decode('utf-8')
#print (r.content.decode('utf-8'))
#rp
#vpd= pd.read_json(r.content.decode('utf-8'))['value']

url = baseurl+"/Variables('bevtot')/GeoLevels"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))

url = baseurl+"/Variables('zonpvtj')/GeoLevels"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
rp= r.content.decode('utf-8')
#print (r.content.decode('utf-8'))
#rp

#reminder: welke geolevers zijn er voor zon
r=getkkmo("/Variables('zonpv_twh_groot_res')/GeoLevels")
r

# +
url = baseurl+"/Variables('cbsapi_70072ned')/GeoLevels('gemeente')/PeriodLevels('YEAR')/Periods('2016')/Values"
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
print (r.content.decode('utf-8'))

#print (getkkmo("/Variables('cbs_zon')/GeoLevels('gemeente')/PeriodLevels('YEAR')/Periods('2016')/Values"))


# +
#nu eens energie per RES regio ophalen
# -

baseyear='2010'
r=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('res')/PeriodLevels('year')/Periods('"+
                baseyear+"')/Values")
#r

baseyear='2010'
r=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('provincie')/PeriodLevels('year')/Periods('"+
                baseyear+"')/Values")
#r

r=getkkmo("/Variables('hern_tot')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
r

totenjaarres=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
totenjaarres['VarName']='energie_totaal_combi'
totenjaarres[totenjaarres['ExternalCode']==u16res]

hernjaarres=getkkmo("/Variables('hern_tot')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
hernjaarres['VarName']='hern_tot'
hernjaarres[hernjaarres['ExternalCode']==u16res]

#deze is in GWh
#bij deze kunnen ook ook gemeentes worden opgevraagd
#print (getkkmo("/Variables('res_bod')/GeoLevels"))
rbod1=getkkmo("/Variables('res_bod')/GeoLevels('res')/PeriodLevels('year')/Periods('all')/Values")
bodresjr= rbod1[rbod1['ValueString']!='?'].copy()
bodresjr['ValueString']= pd.to_numeric(bodresjr['ValueString'],errors='coerce')*3.6
bodresjr['VarName']='RES bod'
bodresjr

r=getkkmo("/Variables('res_pijplijn')/GeoLevels")
r



hernaand=pd.concat([totenjaarres, hernjaarres,bodresjr],copy=False)
hernaand['Jaar']= pd.to_numeric(hernaand['Period'],errors='coerce') 
hernaand['Value']= pd.to_numeric(hernaand['ValueString'],errors='coerce')
hernaand

hernaand_u16=hernaand[hernaand['ExternalCode']==u16res]
sns.lineplot(x='Jaar',y="Value",hue="VarName",data=hernaand_u16)
sns.scatterplot(x='Jaar',y="Value",hue="VarName",data=hernaand_u16)
plt.ylabel('Energie [TJ]')
plt.title('Totaal energieverbruik en hernieuwbare energie U16')

# +
##nu volt productie code

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
refresreg_selbox

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
        ntrunc=row['Name']
        if len(ntrunc) >15:
            ntrunc=(ntrunc[0:10])+"..."
        t1= ntrunc+'\n'+(str(row['refwrd']))
        plt.text(row['PointX'],row['PointY'],t1 , ha='right',va='center') 
    #cx.add_basemap(ax, source= prov0)
    plt.title('Totaal energieverbruik 2010 per '+thislev)
    plt.gca().set_aspect('equal')
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    return fig
r=selboxbasusg(refgem_selbox,glb_regsel)
# -

r=selboxbasusg(refresreg_selbox,'resregs')

refgem_selbox_df = pd.DataFrame(refgem_selbox)
gdf = geopandas.GeoDataFrame(
    refgem_selbox_df, geometry=geopandas.points_from_xy(refgem_selbox_df.PointX, refgem_selbox_df.PointY), 
      crs="epsg:28992"
)
gdf

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
def getrelselbox(colnm,query,decr):
    thislev=refgem_selbox.iloc[0]['GeoLevel']
    thisdat=getkkmo(re.sub('gemeente',thislev,query))
    thisdat['VarName']=colnm
    thisdat_selbox= thisdat.merge( refgem_selbox, how='left', on=selboxmergeon )
    thisdat_selbox= thisdat_selbox[thisdat_selbox['Name'].isna()==False].copy()
    thisdat_selbox['Jaar']= pd.to_numeric(thisdat_selbox['Period'],errors='coerce') 
    thisdat_selbox['Besparing']= (   
       pd.to_numeric(thisdat_selbox['ValueString'],errors='coerce')/ 
       thisdat_selbox['refwrd'] )
    if decr:
        thisdat_selbox['Besparing']=1- thisdat_selbox['Besparing']
    return (thisdat_selbox)

totenjaargem_selbox= getrelselbox ('energie_totaal_combi',
    "/Variables('energie_totaal_combi')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
totenjaargem_selbox
# -

pltdat=totenjaargem_selbox
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie energiebesparing')
plt.title('Energiebesparing '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

hernjaargem_selbox= getrelselbox ('hern_tot',
    "/Variables('hern_tot')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
hernjaargem_selbox

pltdat=hernjaargem_selbox
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie energieopwek')
plt.title('Energiebesparing '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

#deze is in GWh
#REs bod per gemeente: er worden wel records getoond, maar deze bevatten meestal lege strings
#print (getkkmo("/Variables('res_bod')/GeoLevels"))
rbod1gemraw=getkkmo("/Variables('res_bod')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('2030')/Values")
bodresgjr= rbod1gemraw[rbod1gemraw['ValueString']!=''].copy()
bodresgjr['Value']= pd.to_numeric(bodresgjr['ValueString'],errors='coerce')*3.6
bodresgjr['VarName']='RES bod'
bodresgjr

#REs bod per gemeente: er worden wel records getoond, maar deze bevatten geen getallen
resbodgem_selbox_o= getrelselbox ('RES bod',
    "/Variables('res_bod')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('2030')/Values",
                                   False)
#resbodgem_selbox= resbodgem_selbox[ False== resbodgem_selbox['Besparing'] .isna()].copy()
#resbodgem_selbox['Besparing']=  resbodgem_selbox['Besparing']*3.6
resbodgem_selbox_o

#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
resbodgem_aanp =pd.read_excel('../data/resbod_gem_aanp.xlsx')
resbodgem_aanp=resbodgem_aanp[['GeoLevel','Name','Jaar','Besparing']] 
resbodgem_aanp=resbodgem_aanp.rename(columns={'Jaar':'Jaar_aanp','Besparing':'Besparing_aanp' } )
resbodgem_selbox = resbodgem_selbox_o.merge(resbodgem_aanp, on = ['GeoLevel','Name'], how='left')
#de code hieronder KAN NIET kloppen: condities tegenovergesteld
resbodgem_selbox['Jaar'] = resbodgem_selbox['Jaar_aanp'] .where(pd.isna(resbodgem_selbox['Jaar_aanp']),
                           resbodgem_selbox['Jaar'])
resbodgem_selbox['Besparing'] = resbodgem_selbox['Besparing_aanp'] .where(False==pd.isna(resbodgem_selbox['Besparing_aanp']),
                           resbodgem_selbox['Besparing'])
resbodgem_selbox=resbodgem_selbox.drop(['Jaar_aanp','Besparing_aanp' ],axis=1)
resbodgem_selbox

# +
#alleen grootschalige zon en wind vallen onder RES
# -



windtjbrutnormgem_selbox= getrelselbox ('windtjbrutnorm',
    "/Variables('windtjbrutnorm')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
pltdat=windtjbrutnormgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie wind opwek')
plt.title('Wind opwek '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

wind_twh_res_norm_selbox= getrelselbox ('wind_twh_res_norm',
    "/Variables('wind_twh_res_norm')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
wind_twh_res_norm_selbox['Besparing'] = wind_twh_res_norm_selbox['Besparing'] *3600
pltdat=wind_twh_res_norm_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie wind opwek')
plt.title('Wind opwek '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# +
zonpvtjgrootgem_selbox= getrelselbox ('zonpv_twh_groot_res',
    "/Variables('zonpv_twh_groot_res')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
zonpvtjgrootgem_selbox['Besparing']=zonpvtjgrootgem_selbox['Besparing']*3600
#print (zonpvtjgrootgem_selbox)

pltdat=zonpvtjgrootgem_selbox
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie groot zon opwek')
plt.title('Grootschalige zon opwek '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
# -

hernjaargemklein_selbox =hernjaargem_selbox. merge (
    windtjbrutnormgem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "wind_groot"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
     zonpvtjgrootgem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "zon_groot"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ) .copy()   
#                           
hernjaargemklein_selbox['zon_groot']= hernjaargemklein_selbox['zon_groot']. where (
    hernjaargemklein_selbox['Jaar'] > 2019 , 0 )
hernjaargemklein_selbox['VarName'] = 'Hern_klein'
hernjaargemklein_selbox['Besparing'] =hernjaargemklein_selbox['Besparing'] -\
   hernjaargemklein_selbox['wind_groot'] - hernjaargemklein_selbox['zon_groot'] 
#print (hernjaargemklein_selbox)
hernjaargemklein_selbox[hernjaargemklein_selbox['Name']=='Houten']
#hernjaargemklein_selbox[hernjaargemklein_selbox['Jaar']==2010]

#print (windtjbrutnormgem_selbox)
pltdat=hernjaargemklein_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie wind opwek')
plt.title('Hernieuwbaar niet RES tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

#tel nu laatste niet-RES op bij RES bod
resbodgemplusklein_selbox =resbodgem_selbox.copy()
resbodgemplusklein_selbox ['VarName'] = 'RES bod + klein'
hernjaargemklein_max= hernjaargemklein_selbox[['Name','Besparing']].groupby(["Name"]).agg("max").reset_index()
#print(hernjaargemklein_max)
resbodgemplusklein_selbox= resbodgemplusklein_selbox.merge(hernjaargemklein_max.
            rename (columns={"Besparing": "laatsteklein"}) ,how='left',on='Name')
resbodgemplusklein_selbox ['Besparing'] = resbodgemplusklein_selbox ['Besparing'] + resbodgemplusklein_selbox ['laatsteklein']
resbodgemplusklein_selbox

# +
doeljaar_gem =resbodgem_selbox.copy()
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
#als u16: gebruik alleen aangeleverde data, niet voor andere gemeenten
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
if glb_regsel=='u16':
    doeljaar_gem=doeljaar_gem_aanpo
doeljaar_gem['Target']=doeljaar_gem['Besparing']

sdbb=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbb['Jaar']=int (glb_refyear)
sdbb['Target']=0
sdbg=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbg['Jaar']=int (glb_refyear)
sdbg['Target']=1

#doeljaar_gem
# -

selbox_gridwrap=6
gemopwsrt_selbox=pd.concat([ 
    hernjaargem_selbox,
                            windtjbrutnormgem_selbox,
                           wind_twh_res_norm_selbox, zonpvtjgrootgem_selbox,
                            hernjaargemklein_selbox,doeljaar_gem,sdbb  
#                           ],copy=False).sort_values(['Name','Jaar'])
                           ,resbodgem_selbox,resbodgemplusklein_selbox],copy=False)
g= sns.FacetGrid(col="Name",hue="VarName",data=gemopwsrt_selbox,col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing")
g.set(ylim=(0, None))
#g.map(sns.lineplot,'Jaar',"Target",orient='y')
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
g.add_legend()
figname = "../output/gemopwsrt_"+glb_regsel+'.png';
g.savefig(figname,dpi=300) 

gemtotdat_selbox=pd.concat([totenjaargem_selbox, hernjaargem_selbox,
    resbodgemplusklein_selbox,
                            sdbb,doeljaar_gem,sdbg],copy=False)
g= sns.FacetGrid(col="Name",hue="VarName",data=gemtotdat_selbox,col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing")
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/gemtotdat_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")


def pltpadongeo(ref_selbox,my_regsel, datah, datab,doeljaar): 
    param_regiosels=regiosels_df.to_dict('index')[my_regsel]
    fig=selboxbasusg(ref_selbox,glb_regsel)
    for idx, row in pd.DataFrame(doeljaar).iterrows():
        if not pd.isna(row['Jaar']):
            t1= '  '+str(row['Besparing'])+'\n  \''+str(int(row['Jaar'])-2000)
            plt.text(row['PointX'],row['PointY'],t1 , ha='left',va='center') 

    yrmtr=100*param_regiosels['gscale']
    rmtr=5000*param_regiosels['gscale']
    ctryr=2030
    xdat=(datab['Jaar']-ctryr) * yrmtr +datab['PointX']
    ydat=(datab['Besparing']-0.5) * rmtr +datab['PointY']
    plt.scatter(xdat,ydat,label="Verbruik")
    xdat=(datah['Jaar']-ctryr) * yrmtr +datah['PointX']
    ydat=(datah['Besparing']-0.5) * rmtr +datah['PointY']
    plt.scatter(xdat,ydat,label="Opwek")
    sdbk=doeljaar.drop(['Besparing'], axis=1).copy()
    sdbk['Jaar']=np.nan
    sdbk['Target']=-1
    datad=pd.concat([sdbb,doeljaar,sdbg,sdbk],copy=False).sort_values(['Name','Target'])
    xdat=(datad['Jaar']-ctryr) * yrmtr +datad['PointX']
    ydat=(datad['Target']-0.5) * rmtr +datad['PointY']
    plt.plot(xdat,ydat,label="Mogelijk pad")
    #sns.scatterplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.title(param_regiosels['lev']+' naar energieneutraal vanaf 2010')
    figname = "../output/gemtotdatgeo_"+my_regsel+'.png';
    fig.savefig(figname,dpi=300)     
pltpadongeo(refgem_selbox,glb_regsel,hernjaargem_selbox, totenjaargem_selbox,doeljaar_gem)        

# +
#en nu verbruik per sector
# -

energie_gogem_selbox= getrelselbox ('energie_gebouwnde omgeving',
    "/Variables('energie_go')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
pltdat=energie_gogem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie gebouwde omgeving '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_verkeergem_selbox= getrelselbox ('energie_verkeer',
    "/Variables('energie_verkeer')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
energie_verkeergem_selbox= energie_verkeergem_selbox[energie_verkeergem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_verkeergem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie verkeer '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_snelweggem_selbox= getrelselbox ('energie_snelweg',
    "/Variables('energie_snelweg')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                    False)                                            
energie_snelweggem_selbox= energie_snelweggem_selbox[energie_snelweggem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_snelweggem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie snelwegen '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_ldpnt_tot_tjgem_selbox= getrelselbox ('ldpnt_tot_tj',
    "/Variables('ldpnt_tot_tj')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                    False)                                            
#energie_snelweggem_selbox= energie_snelweggem_selbox[energie_snelweggem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_ldpnt_tot_tjgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie publieke laadpalen '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_exvvgem_selbox= getrelselbox ('energie_totaal_ex_vervoer_combi',
    "/Variables('energie_totaal_ex_vervoer_combi')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
pltdat=energie_exvvgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie ex vv '+glb_regsel+' tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# +
energie_vniettak_selbox =totenjaargem_selbox.copy(). merge (
    energie_exvvgem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en niet vv"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
    energie_verkeergem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en verkeer"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
    energie_snelweggem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en snelweg"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] )    
    
#                           
energie_vniettak_selbox['VarName'] = 'Energie VV overig'
energie_vniettak_selbox['Besparing'] =energie_vniettak_selbox['Besparing'] -\
   energie_vniettak_selbox['en niet vv'] - energie_vniettak_selbox['en verkeer'] -\
   energie_vniettak_selbox['en snelweg'] 
energie_vniettak_selbox
# -

pltdat=energie_vniettak_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie verbruik niet in grote cat')
plt.title('Overig verbruik tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# +
energie_restgem_selbox =totenjaargem_selbox.copy(). merge (
    energie_gogem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en geb omg"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
    energie_verkeergem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en verkeer"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
    energie_snelweggem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "en snelweg"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] )    
    
#                           
energie_restgem_selbox['VarName'] = 'Energie overig'
energie_restgem_selbox['Besparing'] =energie_restgem_selbox['Besparing'] -\
   energie_restgem_selbox['en geb omg'] - energie_restgem_selbox['en verkeer'] -\
   energie_restgem_selbox['en snelweg'] 
energie_restgem_selbox
# -

pltdat=energie_restgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie verbruik niet in grote cat')
plt.title('Overig verbruik tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

gemverbsrt_selbox=pd.concat([ totenjaargem_selbox,
        energie_gogem_selbox, energie_restgem_selbox ,
    energie_verkeergem_selbox  ,energie_snelweggem_selbox  ,
                             doeljaar_gem,sdbg   ],copy=False).sort_values(['Name','Jaar'])
#                           ,resbodgem_selbox],copy=False)
g= sns.FacetGrid(col="Name",hue="VarName",data=gemverbsrt_selbox,col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing",)
g.set(ylim=(0, None))
#g.map(sns.lineplot,'Jaar',"Target",orient='y')
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
g.add_legend()
figname = "../output/gemverbsrt_"+glb_regsel+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")


