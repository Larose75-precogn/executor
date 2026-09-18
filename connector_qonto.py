"""Connector Qonto — fetch(compte_brick, api_key) -> [{solde, devise?, date?}, ...] (§8
ARCHITECTURE.md Suivre Mes Comptes). Isolé : si Qonto change son API, seul ce fichier change.

API Qonto (documentation Qonto Partnerships) : base https://thirdparty.qonto.com, auth par
clé API simple "{organization-slug}:{secret-key}" dans le header Authorization (ressemble à
HTTP Basic mais n'en est PAS un — pas de Base64). GET /v2/organization renvoie l'org résolue
depuis la clé elle-même (pas de slug dans l'URL) + son tableau bank_accounts[].

Différence structurelle importante avec Mercury : **une clé API Qonto = une seule
organisation**. deux titulaires différents dans les données d'une org (« Compte pro A », « Compte pro B »)
sont très probablement 2 organisations Qonto distinctes, donc 2 clés API
séparées — jamais une seule clé partagée. Voir `secret_name_for_compte()` dans
executor/app.py : le nom du secret est dérivé du `titulaire` de la brique Compte, pas fixe
comme pour Mercury.
"""
import requests

QONTO_API_BASE = 'https://thirdparty.qonto.com'


def fetch(compte_brick, api_key):
    """Une clé API Qonto authentifie UNE organisation entière (voir docstring du module), pas un
    compte précis — et contrairement à Mercury, le `name` d'un bank_account Qonto est un libellé
    libre choisi par l'utilisateur dans Qonto ("Compte principal", "Compte secondaire", ...) qui ne contient JAMAIS le nom du titulaire ni de notre brique Compte
    (vérifié en conditions réelles, 2026-07-25 : aucun des deux bank_accounts d'une org ne matchait le nom de sa brique Compte). Le matching par nom est donc inutilisable comme
    critère principal ici.

    Stratégie : ne garder que les comptes `status == 'active'` (les comptes clôturés restent
    dans l'API indéfiniment). S'il n'en reste qu'un seul, c'est le bon — la clé API elle-même a
    déjà fait la sélection en amont (1 clé = 1 organisation). S'il en reste plusieurs, on
    retombe sur `main: true`, puis en dernier recours sur un matching de nom."""
    contenu = compte_brick.get('contenu', {})
    nom = (contenu.get('nom') or '').lower()
    etablissement = (contenu.get('etablissement') or '').lower()
    nom_mots_distinctifs = [w for w in nom.split() if len(w) > 3 and w != etablissement]

    r = requests.get(
        f'{QONTO_API_BASE}/v2/organization',
        headers={'Authorization': api_key},
        timeout=15,
    )
    r.raise_for_status()
    accounts = r.json().get('organization', {}).get('bank_accounts', [])
    actifs = [a for a in accounts if a.get('status') == 'active']

    match = None
    if len(actifs) == 1:
        match = actifs[0]
    else:
        pool = actifs or accounts
        for acc in pool:
            if acc.get('main'):
                match = acc
                break
        if not match:
            for acc in pool:
                label = (acc.get('name') or '').lower()
                if label and (label in nom or nom in label or any(word in label for word in nom_mots_distinctifs)):
                    match = acc
                    break

    if not match:
        raise ValueError(f"Aucun compte Qonto actif ne correspond à '{contenu.get('nom')}' parmi {len(accounts)} comptes renvoyés par l'API")

    return [{'solde': match['balance'], 'devise': match.get('currency', 'EUR')}]


def fetch_transactions(compte_brick, api_key, per_page=100, max_pages=20):
    """JournaldeBanque : historique DETAILLE des transactions d'un compte Qonto (en plus du
    solde renvoye par fetch()). Meme convention de sortie que connector_powens /
    connector_enablebanking.fetch_transactions -> liste normalisee
    {date, montant_signe, devise, libelle, source_id, _raw}, pour que jdb_api reste agnostique
    du connector. API Qonto : GET /v2/transactions?bank_account_id=<id> (auth identique a fetch)."""
    # Meme selection de compte que fetch() : 1 cle Qonto = 1 organisation, on prend le compte actif.
    r = requests.get(f'{QONTO_API_BASE}/v2/organization',
                     headers={'Authorization': api_key}, timeout=15)
    r.raise_for_status()
    accounts = r.json().get('organization', {}).get('bank_accounts', [])
    actifs = [a for a in accounts if a.get('status') == 'active'] or accounts
    match = None
    if len(actifs) == 1:
        match = actifs[0]
    else:
        for a in actifs:
            if a.get('main'):
                match = a
                break
        match = match or (actifs[0] if actifs else None)
    if not match:
        raise ValueError("Aucun compte Qonto actif pour recuperer les transactions")
    account_id = match.get('id')

    normalized = []
    page = 1
    while page and page <= max_pages:
        rr = requests.get(f'{QONTO_API_BASE}/v2/transactions',
                          headers={'Authorization': api_key},
                          params={'bank_account_id': account_id, 'per_page': per_page, 'page': page},
                          timeout=20)
        rr.raise_for_status()
        payload = rr.json()
        txs = payload.get('transactions', [])
        for t in txs:
            amount = float(t.get('amount') or 0)
            signed = amount if t.get('side') == 'credit' else -amount
            libelle = (t.get('clean_counterparty_name') or t.get('label')
                       or t.get('reference') or '').strip()
            date = (t.get('settled_at') or t.get('emitted_at') or '')[:10]
            normalized.append({
                'date': date,
                'montant_signe': round(signed, 2),
                'devise': t.get('currency') or match.get('currency') or 'EUR',
                'libelle': libelle,
                'source_id': f"qonto_{t.get('transaction_id') or t.get('id')}",
                '_raw': t,
            })
        if not txs:
            break
        page = (payload.get('meta') or {}).get('next_page')
    return normalized
