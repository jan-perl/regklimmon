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

import pandas as pd
import numpy as np
import subprocess
import requests
import json
import os
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
print (rdf1)

url = baseurl+'/GeoLevels' 
headers = {'apikey': myapikey} 
r = requests.get(url, headers=headers)
vpd= pd.read_json(r.content)['value']
print (vpd)

pval=pd.json_normalize(vpd)
print(pval)


# +
def getkkmo(src):
    nextlinkstr='@odata.nextLink'    
    url = baseurl+src
    headers = {'apikey': myapikey}
    getblk=True
    firstblk=True
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
        if firstblk:
            firstblk=False
            pval=[bval]
        else:
            pval.append(bval)
#    print(pval)        
    return pd.concat(pval)

print (getkkmo('/GeoLevels'))
# -

#haal namen RES regios op
GeoLevels_res =getkkmo("/GeoLevels('res')/GeoItems")
print (GeoLevels_res)
u16res='res_14'

# +
#utr left=113000, right=180000)
#ax.set_ylim(bottom=480000, top=430000)
#u10     ax.set_xlim(left=125000, right=152000)
#   ax.set_ylim(bottom=442000, top=468000)

# +
GeoLevels_gem =getkkmo("/GeoLevels('gemeente')/GeoItems")
GeoLevels_gem_selbox = GeoLevels_gem [(GeoLevels_gem['PointX']> 119000 ) &
                                    (GeoLevels_gem['PointX']< 154000 ) &
                                    (GeoLevels_gem['PointY']> 438000 ) &
                                    (GeoLevels_gem['PointY']< 471000 ) ]
                                    
print (GeoLevels_gem_selbox)
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
print (getkkmo("/Variables('energie_totaal_combi')/GeoLevels('res')/PeriodLevels('year')/Periods('"+
                baseyear+"')/Values"))

baseyear='2010'
print (getkkmo("/Variables('energie_totaal_combi')/GeoLevels('provincie')/PeriodLevels('year')/Periods('"+
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
refyear='2010'
refgementot=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('"+
               refyear+"')/Values")
refgem_selbox= refgementot.merge( GeoLevels_gem_selbox, how='left',                
                                 on='ExternalCode')
refgem_selbox= refgem_selbox[refgem_selbox['Name'].isna()==False]
refgem_selbox['refwrd']=pd.to_numeric(refgem_selbox['ValueString'])
refgem_selbox['refperiod']=pd.to_numeric(refgem_selbox['Period'])
refgem_selbox=refgem_selbox.drop(['Period','ValueString','Description'], axis=1)
print (refgem_selbox) 

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
    
def selboxbasusg():
    svals= refgem_selbox['refwrd']*0.5
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.scatter(refgem_selbox['PointX'],refgem_selbox['PointY'],s=svals,alpha=.1) 
    for idx, row in pd.DataFrame(refgem_selbox).iterrows():
        plt.text(row['PointX'],row['PointY'],
                 row['Name']+'\n'+(str(row['refwrd'])),
                 ha='right',va='center') 
    #cx.add_basemap(ax, source= prov0)
    plt.title('Totaal energieverbruik 2010 per gemeente')
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(plaxkm))
    return fig
r=selboxbasusg()
# -

refgem_selbox_df = pd.DataFrame(refgem_selbox)
gdf = geopandas.GeoDataFrame(
    refgem_selbox_df, geometry=geopandas.points_from_xy(refgem_selbox_df.PointX, refgem_selbox_df.PointY), 
      crs="epsg:28992"
)
print(gdf)

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
    thisdat=getkkmo(query)
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
print (totenjaargem_selbox)
# -

pltdat=totenjaargem_selbox
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie energiebesparing')
plt.title('Energiebesparing U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

hernjaargem_selbox= getrelselbox ('hern_tot',
    "/Variables('hern_tot')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
print (hernjaargem_selbox)

pltdat=hernjaargem_selbox
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie energieopwek')
plt.title('Energiebesparing U10 tov referentiejaar')
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
resbodgem_selbox= getrelselbox ('RES bod',
    "/Variables('res_bod')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('2030')/Values",
                                   False)
#resbodgem_selbox= resbodgem_selbox[ False== resbodgem_selbox['Besparing'] .isna()].copy()
#resbodgem_selbox['Besparing']=  resbodgem_selbox['Besparing']*3.6
print (resbodgem_selbox)

# +
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
    
resbodgem_selbox =pd.read_excel('../data/resbod_gem_aanp.xlsx')

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
plt.title('Wind opwek U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

wind_twh_res_norm_selbox= getrelselbox ('wind_twh_res_norm',
    "/Variables('wind_twh_res_norm')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
wind_twh_res_norm_selbox['Besparing'] = wind_twh_res_norm_selbox['Besparing'] *3600
pltdat=wind_twh_res_norm_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie wind opwek')
plt.title('Wind opwek U10 tov referentiejaar')
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
plt.title('Grootschalige zon opwek U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
# -

hernjaargemklein_selbox =hernjaargem_selbox.copy(). merge (
    windtjbrutnormgem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "wind_groot"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] ). merge (
     zonpvtjgrootgem_selbox[selboxmergeon+['Jaar','Besparing']].rename (columns={"Besparing": "zon_groot"}),
                                                    how='left' ,on=selboxmergeon+['Jaar'] )    
#                           
hernjaargemklein_selbox['VarName'] = 'Hern_klein'
hernjaargemklein_selbox['Besparing'] =hernjaargemklein_selbox['Besparing'] -\
   hernjaargemklein_selbox['wind_groot'] - hernjaargemklein_selbox['zon_groot'] 
print (hernjaargemklein_selbox)

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
print(resbodgemplusklein_selbox)

gemopwsrt_selbox=pd.concat([ hernjaargem_selbox,
                            windtjbrutnormgem_selbox,
                           wind_twh_res_norm_selbox, zonpvtjgrootgem_selbox,
                            hernjaargemklein_selbox,doeljaar_gem,sdbb  
#                           ],copy=False).sort_values(['Name','Jaar'])
                           ,resbodgem_selbox,resbodgemplusklein_selbox],copy=False)
g= sns.FacetGrid(col="Name",hue="VarName",data=gemopwsrt_selbox,col_wrap=6,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing",)
g.set(ylim=(0, None))
#g.map(sns.lineplot,'Jaar',"Target",orient='y')
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
g.add_legend()
figname = "../output/gemopwsrt"+'.png';
g.savefig(figname,dpi=300) 

# +
#maak nu zelf doelstellingen records
# -

doeljaar_gem =resbodgem_selbox.copy()
doeljaar_gem ['Jaar'] = 2050
doeljaar_gem ['VarName'] = 'ENeutr Doel'
doeljaar_gem ['Jaar'] = doeljaar_gem ['Jaar'].where (
    doeljaar_gem ['Name']!= 'Wijk bij Duurstede' , 2030 )
doeljaar_gem ['Besparing'] =0.5
doeljaar_gem.to_excel('../intermediate/doeljaar_gem_auto.xlsx')
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
doeljaar_gem=pd.read_excel('../data/doeljaar_gem_aanp.xlsx')
print (doeljaar_gem)

# +
doeljaar_gem['Target']=doeljaar_gem['Besparing']
sdbb=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbb['Jaar']=2010
sdbb['Target']=0
sdbg=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbg['Jaar']=2010
sdbg['Target']=1

gemtotdat_selbox=pd.concat([totenjaargem_selbox, hernjaargem_selbox,resbodgemplusklein_selbox,
                            sdbb,doeljaar_gem,sdbg],copy=False)
g= sns.FacetGrid(col="Name",hue="VarName",data=gemtotdat_selbox,col_wrap=6,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing")
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/gemtotdat"+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")
# -

fig=selboxbasusg()
datah=hernjaargem_selbox
datab=totenjaargem_selbox
yrmtr=100
rmtr=5000
ctryr=2030
xdat=(datab['Jaar']-ctryr) * yrmtr +datab['PointX']
ydat=(datab['Besparing']-0.5) * rmtr +datab['PointY']
plt.scatter(xdat,ydat,label="Verbruik")
xdat=(datah['Jaar']-ctryr) * yrmtr +datah['PointX']
ydat=(datah['Besparing']-0.5) * rmtr +datah['PointY']
plt.scatter(xdat,ydat,label="Opwek")
sdbk=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbk['Jaar']=np.nan
sdbk['Target']=-1
datad=pd.concat([sdbb,doeljaar_gem,sdbg,sdbk],copy=False).sort_values(['Name','Target'])
xdat=(datad['Jaar']-ctryr) * yrmtr +datad['PointX']
ydat=(datad['Target']-0.5) * rmtr +datad['PointY']
plt.plot(xdat,ydat,label="Mogelijk pad")
#sns.scatterplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.title('Gemeentes naar energieneutraal vanaf 2010')
figname = "../output/gemtotdatgeo"+'.png';
fig.savefig(figname,dpi=300) 

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
plt.title('Energie gebouwde omgeving U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_verkeergem_selbox= getrelselbox ('energie_verkeer',
    "/Variables('energie_verkeer')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
energie_verkeergem_selbox= energie_verkeergem_selbox[energie_verkeergem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_verkeergem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie verkeer U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_snelweggem_selbox= getrelselbox ('energie_snelweg',
    "/Variables('energie_snelweg')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                    False)                                            
energie_snelweggem_selbox= energie_snelweggem_selbox[energie_snelweggem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_snelweggem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie snelwegen U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_ldpnt_tot_tjgem_selbox= getrelselbox ('ldpnt_tot_tj',
    "/Variables('ldpnt_tot_tj')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                    False)                                            
#energie_snelweggem_selbox= energie_snelweggem_selbox[energie_snelweggem_selbox['Jaar']>=2010]
#print (windtjbrutnormgem_selbox)
pltdat=energie_ldpnt_tot_tjgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie publieke laadpalen U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

energie_exvvgem_selbox= getrelselbox ('energie_totaal_ex_vervoer_combi',
    "/Variables('energie_totaal_ex_vervoer_combi')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
#print (windtjbrutnormgem_selbox)
pltdat=energie_exvvgem_selbox.copy()
sns.lineplot(x='Jaar',y="Besparing",hue="Name",data=pltdat)
plt.ylabel('Fractie tov referentiejaar')
plt.title('Energie ex vv U10 tov referentiejaar')
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
print (energie_vniettak_selbox)
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
print (energie_restgem_selbox)
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
g= sns.FacetGrid(col="Name",hue="VarName",data=gemverbsrt_selbox,col_wrap=6,
                   sharex=True, sharey=True)
g.map(sns.scatterplot,'Jaar',"Besparing",)
g.set(ylim=(0, None))
#g.map(sns.lineplot,'Jaar',"Target",orient='y')
g.map(sns.lineplot,'Jaar',"Target",estimator=None,sort=False,alpha=0.5)
#plt.ylabel('Energie [TJ]')
g.add_legend()
figname = "../output/gemverbsrt"+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")


