/** Detect a previously started render without attempting another uncertain upload. */
export function isExistingRender(created){
 const msg=String(created?.data?.message||created?.data?.error||"");
 return created?.status===409 || /(?:already|gi[aà]\s+(?:esiste|render|avviat[ao])|duplicate|duplicat|request[_ -]?id.*(?:used|esiste))/i.test(msg);
}
