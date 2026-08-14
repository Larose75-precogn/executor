"""Connector Powens — fetch(compte_brick, credentials) -> [{solde, devise}]
(§8 ARCHITECTURE.md Suivre Mes Comptes). Agrégateur DSP2 français (ex-Budget Insight,
powens.com), utilisé pour Banque BCP — ni Enable Banking (établissement absent de leur liste
~2600 ASPSPs, vérifié 2026-07-26) ni un accès direct (même mur réglementaire AISP/eIDAS que
Crédit Mutuel) ne le couvraient.

Authentification : `POST /auth/init` avec client_id+client_secret (app Powens "smc", domaine
`smc-sandbox.biapi.pro` — nom "sandbox" trompeur, connecte de VRAIES banques avec de VRAIES
données, vérifié avec le vrai compte Banque BCP de Stéphane, 2569,99€ récupérés en conditions
réelles). **Piège réel trouvé en testant** : `/auth/init` n'est PAS idempotent — chaque appel
crée un NOUVEL utilisateur Powens (`id_user` incrémente à chaque fois, 1 puis 3 puis 4...),
comme une inscription, pas un rafraîchissement de token. Le connecteur qui appelait `/auth/init`
à chaque synchro se retrouvait donc systématiquement avec un utilisateur tout neuf, sans la
connexion BCP déjà établie (`GET /users/me/accounts` vide). Le `auth_token` permanent obtenu la
toute première fois (lié à l'utilisateur qui a réellement la connexion bancaire) doit être
stocké tel quel et réutilisé indéfiniment — jamais régénéré par un connector en usage normal.

Contrairement à Enable Banking, Powens expose bien un endpoint `GET /users/me/accounts` qui
liste TOUS les comptes déjà connectés à tout moment (pas de contrainte "capturer une fois au
moment de la connexion") — mais comme un même token peut à terme couvrir plusieurs connexions
bancaires différentes (une par établissement lié), il faut quand même savoir QUEL compte Powens
correspond à quelle brique Compte : pas de matching par nom fiable ici non plus (`original_name`
Powens, ex. "CPT DEPOT PART.", ne contient ni l'établissement ni le titulaire) — `powens_account_id`
persisté sur la brique Compte au moment de la connexion, même logique que
`enablebanking_account_uid`.
"""
import json

import requests


def fetch(compte_brick, credentials_json):
    """`credentials_json` : secret org `powens_credentials`, JSON sérialisé
    {"domain": "smc-sandbox", "auth_token": "..."} — le token PERMANENT capturé lors de la 1re
    connexion (voir docstring du module), jamais client_id/client_secret régénérant un nouvel
    utilisateur à chaque appel."""
    credentials = json.loads(credentials_json)
    account_id = compte_brick.get('contenu', {}).get('powens_account_id')
    if not account_id:
        raise ValueError("Brique Compte sans powens_account_id — mapping à faire une fois via le flux de connexion Powens (webview)")

    base = f"https://{credentials['domain']}.biapi.pro/2.0"
    headers = {'Authorization': f"Bearer {credentials['auth_token']}"}

    r2 = requests.get(f'{base}/users/me/accounts', headers=headers, timeout=15)
    r2.raise_for_status()
    accounts = r2.json().get('accounts', [])

    match = next((a for a in accounts if a.get('id') == account_id), None)
    if not match:
        raise ValueError(f"Compte Powens id={account_id} introuvable parmi {len(accounts)} comptes renvoyés")

    return [{'solde': match['balance'], 'devise': match['currency']['id']}]


def fetch_transactions(compte_brick, credentials_json, limit=200, max_pages=10):
    """JournaldeBanque (2026-08-14) — historique DÉTAILLÉ des transactions d'un compte déjà
    lié, jamais consommé jusqu'ici (`fetch()` ci-dessus ne remonte que le solde courant).
    Vérifié en HTTP direct, lecture seule, avec de VRAIES données smcspl (2026-08-13, voir
    ~/projects/jdb/CLAUDE.md) : `GET /users/me/accounts/{id}/transactions` répond avec
    `first_date`/`last_date`, pagination par `cursor` dans `_links.next`, et pour chaque
    transaction `id`, `date`, `value` (déjà signé, débit négatif/crédit positif), `wording`/
    `simplified_wording`/`original_wording`, `state`.

    Retourne une liste normalisée `{date, montant_signe, devise, libelle, source_id}` — même
    convention que `connector_enablebanking.fetch_transactions` pour que `jdb_api` reste
    agnostique du connector d'origine (Connector -> Adaptateur -> forme normalisée, jamais de
    JSON brut fournisseur transmis tel quel plus loin, cf. mandat PreCogn modèle/vue).
    `limit`/`max_pages` : garde-fous (un compte avec des années d'historique ne doit jamais
    boucler indéfiniment côté serveur) — 10 pages * 200 = 2000 transactions max par appel,
    largement suffisant pour un relevé/synchro incrémentale ; l'appelant (jdb_api) dédoublonne
    de toute façon par `source_id`, un rappel ultérieur peut donc toujours combler le reste."""
    credentials = json.loads(credentials_json)
    account_id = compte_brick.get('contenu', {}).get('powens_account_id')
    if not account_id:
        raise ValueError("Brique Compte sans powens_account_id — mapping à faire une fois via le flux de connexion Powens (webview)")

    base = f"https://{credentials['domain']}.biapi.pro/2.0"
    headers = {'Authorization': f"Bearer {credentials['auth_token']}"}

    url = f'{base}/users/me/accounts/{account_id}/transactions'
    params = {'limit': limit}
    transactions_raw = []
    devise = None

    for _ in range(max_pages):
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        payload = r.json()
        page = payload.get('transactions', [])
        transactions_raw.extend(page)
        if devise is None and page:
            devise = (page[0].get('currency') or {}).get('id')
        next_link = ((payload.get('_links') or {}).get('next') or {}).get('href')
        if not next_link or not page:
            break
        url = next_link
        params = None  # le cursor est déjà encodé dans next_link

    normalized = []
    for t in transactions_raw:
        libelle = t.get('simplified_wording') or t.get('wording') or t.get('original_wording') or ''
        normalized.append({
            'date': t.get('date'),
            'montant_signe': t.get('value'),
            'devise': (t.get('currency') or {}).get('id') or devise or 'EUR',
            'libelle': libelle.strip(),
            'source_id': f"powens_{t.get('id')}",
            '_raw': t,  # objet brut Powens tel que reçu — consommé UNIQUEMENT par l'appelant
                        # (executor/app.py::_fetch_transactions_for_compte) pour le document
                        # source sanctuarisé (§2bis), jamais transmis tel quel à jdb_api.
        })

    return normalized
