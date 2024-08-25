# Sélection du calendrier (cron)
schedule_options_Hebdo = {
    'Quotidien': '@daily',
    'Hebdomadaire': '@weekly',
    'H.TOUS_LES_DIMANCHES':'0 0 * * 0',  
    'H.TOUS_LES_LUNDIS':'0 0 * * 1',
    'H.TOUS_LES_MARDIS':'0 0 * * 2',
    'H.TOUS_LES_MERCREDIS':'0 0 * * 3',
    'H.TOUS_LES_JEUDIS':'0 0 * * 4',
    'H.TOUS_LES_VENDREDIS':'0 0 * * 5',
    'Personnalisé': 'personnalisé'
}

schedule_options_Annuel = {
    'A_MOIS_DE_DECEMBRE': '0 0 * 12 *',
    'A_MOIS_DE_JANVIER': '0 0 * 1 *',
    'Personnalisé': 'personnalisé'
}