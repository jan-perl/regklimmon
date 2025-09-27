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
GeoLevels_gem_selbox_randst = GeoLevels_gem [(GeoLevels_gem['PointX']> 79000 ) &
                                    (GeoLevels_gem['PointX']< 174000 ) &
                                    (GeoLevels_gem['PointY']> 398000 ) &
                                    (GeoLevels_gem['PointY']< 491000 ) ]
GeoLevels_gem_selbox_nw = GeoLevels_gem [(GeoLevels_gem['PointX']> 10000 ) &
                                    (GeoLevels_gem['PointX']< 200000 ) &
                                    (GeoLevels_gem['PointY']> 490000 ) &
                                    (GeoLevels_gem['PointY']< 700000 ) ]
GeoLevels_gem_selbox_no = GeoLevels_gem [(GeoLevels_gem['PointX']> 170000 ) &
                                    (GeoLevels_gem['PointX']< 300000 ) &
                                    (GeoLevels_gem['PointY']> 490000 ) &
                                    (GeoLevels_gem['PointY']< 700000 ) ]
GeoLevels_gem_selbox_mo = GeoLevels_gem [(GeoLevels_gem['PointX']> 119000 ) &
                                    (GeoLevels_gem['PointX']< 300000 ) &
                                    (GeoLevels_gem['PointY']> 398000 ) &
                                    (GeoLevels_gem['PointY']< 491000 ) ]
GeoLevels_gem_selbox_zo = GeoLevels_gem [(GeoLevels_gem['PointX']> 119000 ) &
                                    (GeoLevels_gem['PointX']< 300000 ) &
                                    (GeoLevels_gem['PointY']> 190000 ) &
                                    (GeoLevels_gem['PointY']< 400000 ) ]
                                    
                                    
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
refyear='2010'
refgementot=getkkmo("/Variables('energie_totaal_combi')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('"+
               refyear+"')/Values")
refgem_selbox= refgementot.merge( GeoLevels_gem_selbox, how='left',                
                                 on='ExternalCode')
refgem_selbox= refgem_selbox[refgem_selbox['Name'].isna()==False]
refgem_selbox['refwrd']=pd.to_numeric(refgem_selbox['ValueString'],errors='coerce')
refgem_selbox['refperiod']=pd.to_numeric(refgem_selbox['Period'],errors='coerce')
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
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
    
resbodgem_selbox =pd.read_excel('../data/resbod_gem_aanp.xlsx')

# +
doeljaar_gem =resbodgem_selbox.copy()
doeljaar_gem ['Jaar'] = 2050
doeljaar_gem ['VarName'] = 'ENeutr Doel'
doeljaar_gem ['Jaar'] = doeljaar_gem ['Jaar'].where (
    doeljaar_gem ['Name']!= 'Wijk bij Duurstede' , 2030 )
doeljaar_gem ['Besparing'] =0.5
doeljaar_gem.to_excel('../intermediate/doeljaar_gem_auto.xlsx')
#overgetypt uit https://portal.ibabs.eu/Document/ListEntry/983e7c96-2e59-4bdf-b3e6-d2f059b2cb8d/e04a3258-ea5e-44b6-990b-53b81d16b14f
doeljaar_gem=pd.read_excel('../data/doeljaar_gem_aanp.xlsx')
doeljaar_gem['Target']=doeljaar_gem['Besparing']
sdbb=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbb['Jaar']=2010
sdbb['Target']=0
sdbg=doeljaar_gem.drop(['Besparing'], axis=1).copy()
sdbg['Jaar']=2010
sdbg['Target']=1


print (doeljaar_gem)

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
print (eml_kb_aant_inr_selbox)
# -

pltdat=eml_kb_aant_inr_selbox
sns.lineplot(x='Jaar',y="Waarde",hue="Name",data=pltdat)
plt.ylabel('Fractie energiebesparing')
plt.title('Energiebesparing U10 tov referentiejaar')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# +
rlevsel=[3,2,1]
olst=[]
ilst=[]
for rlev in rlevsel:
    rlet=str(rlev)
    t= getvalselbox ('eml_kb_perc_r'+rlet,
    "/Variables('eml_kb_perc_r"+rlet+"')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
    t['Rubriceringscode']=rlev
    ilst.append(t)
    slst=pd.concat(ilst)
    sv2= slst[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
    sv2=sv2.rename(columns={"Waarde":"SomWaarde" })
    t=t.merge(sv2,on=["Name",'Jaar'],how='left')               
    olst.append(t)

eml_kb_perc_rx_selbox=pd.concat(olst)
if False:    
    eml_kb_perc_rx_selbox45= eml_kb_perc_rx_selbox[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
    eml_kb_perc_rx_selbox45['Rubriceringscode']= 45
    eml_kb_perc_rx_selbox45['Waarde']= 100- eml_kb_perc_rx_selbox45['Waarde']
    eml_kb_perc_rx_selbox=pd.concat(olst+[eml_kb_perc_rx_selbox45])
# -

selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="Rubriceringscode",
                 data=eml_kb_perc_rx_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/perc_rx"+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

# +
rlevsel=['g','f','e','d','c','b','a']
olst=[]
ilst=[]
for rlev in rlevsel:
    rlet=str(rlev)
    t= getvalselbox ('pwonlab'+rlet,
    "/Variables('pwonlab"+rlet+"')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
    t['pwonlab']=rlev
    ilst.append(t)
    slst=pd.concat(ilst)
    sv2= slst[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
    sv2=sv2.rename(columns={"Waarde":"SomWaarde" })
    t=t.merge(sv2,on=["Name",'Jaar'],how='left')               
    olst.append(t)

pwonlab_selbox=pd.concat(olst).sort_values("pwonlab")
# -



selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="pwonlab",
                 data=pwonlab_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/pwonlab"+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

# +
rlevsel=['g','f','e','d','c','b','atot']
rlevsel=['g','f','e','d','c','b','a','aa','aaa','a4','a5']
olst=[]
ilst=[]
for rlev in rlevsel:
    rlet=str(rlev)
    t= getvalselbox ('geblab'+rlet,
    "/Variables('geblab"+rlet+"')/GeoLevels('gemeente')/PeriodLevels('year')/Periods('all')/Values",
                                   False)
    t['geblab']=rlev
    ilst.append(t)
    slst=pd.concat(ilst)
    sv2= slst[['Name','Jaar','Waarde']].groupby(["Name",'Jaar']).agg("sum").reset_index()
    sv2=sv2.rename(columns={"Waarde":"SomWaarde" })
    t=t.merge(sv2,on=["Name",'Jaar'],how='left')               
    olst.append(t)

geblab_selbox=pd.concat(olst).sort_values("geblab")
# -

#dit moet nog genomeerd op aantallen BAG adressen
selbox_gridwrap=6
g= sns.FacetGrid(col="Name",hue="geblab",
                 data=geblab_selbox, col_wrap=selbox_gridwrap,
                   sharex=True, sharey=True)
g.map(sns.barplot,'Jaar',"SomWaarde")
#plt.ylabel('Energie [TJ]')
#plt.title('Besparing en opwek per gemeente sinds 2010')
g.add_legend()
figname = "../output/geblab"+'.png';
g.savefig(figname,dpi=300, bbox_inches="tight")

fig=selboxbasusg()
datab=eml_kb_perc_rx_selbox[eml_kb_perc_rx_selbox["Rubriceringscode"]==2]
datah=eml_kb_perc_rx_selbox[eml_kb_perc_rx_selbox["Rubriceringscode"]==3]
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
plt.title('Gerapporteerde EML rubricering vanaf 2010')
figname = "../output/eml_kb_perc_rx_geo"+'.png';
fig.savefig(figname,dpi=300) 

fig=selboxbasusg()
datab=pwonlab_selbox[pwonlab_selbox["pwonlab"]=="d"]
datah=pwonlab_selbox[pwonlab_selbox["pwonlab"]=="b"]
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
plt.title('Gerapporteerde EML rubricering vanaf 2010')
figname = "../output/pwonlab_geo"+'.png';
fig.savefig(figname,dpi=300) 


