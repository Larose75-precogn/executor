"""Connector Enable Banking — fetch(compte_brick, credentials) -> [{solde, devise}]
(§8 ARCHITECTURE.md Suivre Mes Comptes). Agrégateur DSP2/PSD2 agréé AISP (enablebanking.com) :
contrairement à Mercury/Qonto, pas de clé API statique par compte — chaque compte est rattaché
au consentement PSD2 (SCA) donné une fois par l'utilisateur sur le site de sa banque.

Authentification : JWT RS256 signé par la clé privée de l'application (générée par Enable
Banking à l'enregistrement de l'app, jamais rotée automatiquement), `kid` = application_id.
Un seul jeu app_id + clé privée sert pour TOUTES les banques/comptes connectés via Enable
Banking, quel que soit l'établissement — ce n'est PAS un connector "Crédit Mutuel", c'est un
connector générique réutilisable pour n'importe quel établissement européen supporté par Enable
Banking (2026-07-25/26, retour de Stéphane après avoir lié Crédit Mutuel).

Contrainte réelle de l'API découverte en testant : il n'existe PAS d'endpoint pour lister les
comptes déjà autorisés d'une session existante avec leurs détails (IBAN/nom) — seul le retour
de `POST /sessions` (au moment de l'échange du code d'autorisation) donne cette liste complète.
Conséquence : contrairement à Mercury/Qonto (résolution par nom à CHAQUE appel, jamais stockée),
ici l'`account_uid` Enable Banking DOIT être capturé une fois et persisté sur la brique Compte
(`contenu.enablebanking_account_uid`) — sinon plus aucun moyen de le retrouver. Exception
documentée et justifiée par une limite d'API, pas un choix de conception.
"""
import json
import time
from datetime import datetime, timedelta, timezone

import jwt
import requests

EB_API_BASE = 'https://api.enablebanking.com'


def sign_jwt(credentials):
    """Jeton RS256 valable 1h pour authentifier un appel à l'API Enable Banking — factorisé
    (2026-07-27) pour être réutilisé par le flux d'autorisation (start_auth/exchange_code,
    ci-dessous) en plus de fetch(), plutôt que dupliqué."""
    iat = int(time.time())
    return jwt.encode(
        {'iss': 'enablebanking.com', 'aud': 'api.enablebanking.com', 'iat': iat, 'exp': iat + 3600},
        credentials['private_key'],
        algorithm='RS256',
        headers={'kid': credentials['app_id']},
    )


def start_auth(credentials, aspsp_name, aspsp_country, psu_id, redirect_url, state, language=None):
    """Démarre une autorisation PSD2 (POST /auth) — renvoie l'URL vers laquelle rediriger
    l'utilisateur pour qu'il choisisse/consente sur le site de sa banque (ou du Mock ASPSP en
    sandbox, "aucun identifiant requis" d'après la doc officielle). `state` : chaîne opaque
    renvoyée telle quelle dans le callback, utilisée par l'appelant pour retrouver l'org/email
    en attente (jamais stockée par Enable Banking lui-même)."""
    token = sign_jwt(credentials)
    body = {
        'aspsp': {'name': aspsp_name, 'country': aspsp_country},
        'psu_type': 'personal',
        'redirect_url': redirect_url,
        'state': state,
        'access': {'valid_until': (datetime.now(timezone.utc) + timedelta(days=90)).strftime('%Y-%m-%dT%H:%M:%S.000Z')},
        'psu_id': psu_id,
    }
    if language:
        body['language'] = language

    r = requests.post(f'{EB_API_BASE}/auth', json=body, headers={'Authorization': f'Bearer {token}'}, timeout=15)
    r.raise_for_status()
    return r.json()['url']


def exchange_code(credentials, code):
    """Échange le `code` reçu sur le callback (POST /sessions) contre la liste des comptes
    autorisés — SEUL moment où Enable Banking renvoie account_uid + IBAN (voir docstring de
    fetch() plus bas : aucun endpoint pour les retrouver après coup)."""
    token = sign_jwt(credentials)
    r = requests.post(f'{EB_API_BASE}/sessions', json={'code': code}, headers={'Authorization': f'Bearer {token}'}, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch(compte_brick, credentials_json):
    """`credentials_json` : secret org `enablebanking_credentials`, JSON sérialisé
    {"app_id": "...", "private_key": "-----BEGIN PRIVATE KEY-----..."} — partagé par tous les
    comptes Enable Banking de l'org, quel que soit l'établissement (voir docstring du module).
    Désérialisé ici (comme `_org_smtp_secret` dans app.py) plutôt que par l'appelant, pour que
    `_sync_one_compte` reste générique (un secret est toujours une chaîne côté registre)."""
    credentials = json.loads(credentials_json)
    account_uid = compte_brick.get('contenu', {}).get('enablebanking_account_uid')
    if not account_uid:
        raise ValueError("Brique Compte sans enablebanking_account_uid — mapping à faire une fois via le flux d'autorisation Enable Banking")

    token = sign_jwt(credentials)

    r = requests.get(
        f'{EB_API_BASE}/accounts/{account_uid}/balances',
        headers={'Authorization': f'Bearer {token}'},
        timeout=15,
    )
    r.raise_for_status()
    balances = r.json().get('balances', [])

    # AccountingBalance (comptable, `balance_type: CLBD`) plutôt que AvailableBalance : cohérent
    # avec la sémantique ledger-cli existante (constat comptable, pas plafond de découvert
    # disponible) — en pratique identiques sur les comptes testés (2026-07-26), mais le choix
    # doit rester déterministe si un jour ils divergent (découvert autorisé en cours d'usage).
    match = next((b for b in balances if b.get('name') == 'AccountingBalance'), None) or (balances[0] if balances else None)
    if not match:
        raise ValueError(f"Aucun solde renvoyé par Enable Banking pour le compte {account_uid}")

    return [{'solde': float(match['balance_amount']['amount']), 'devise': match['balance_amount']['currency']}]


def fetch_transactions(compte_brick, credentials_json, page_limit=200, max_pages=10):
    """JournaldeBanque (2026-08-14) — historique DÉTAILLÉ des transactions d'un compte déjà
    lié, jamais consommé jusqu'ici (`fetch()` ci-dessus ne remonte que le solde). Vérifié en
    HTTP direct, lecture seule, avec de VRAIES données smcspl (2026-08-13, Crédit Mutuel EURL
    SPL, voir ~/projects/jdb/CLAUDE.md) : `GET /accounts/{uid}/transactions` répond `200`,
    format PSD2/CAMT-like — `transaction_amount.{amount,currency}`, `credit_debit_indicator`
    (DBIT/CRDT), `booking_date`/`value_date`/`transaction_date` (3 dates distinctes, on garde
    `booking_date` par défaut — c'est la date qui fait foi côté banque, comme pour un relevé
    papier), `remittance_information` (liste de lignes de libellé), `status`.

    Pagination : non vérifiée en conditions réelles au-delà de la 1re page (le compte de test
    n'avait pas assez d'historique pour la déclencher) — implémentée par précaution selon le
    contrat PSD2 standard (`continuation_key` dans la réponse, à repasser en paramètre de
    requête), avec un garde-fou dur (`max_pages`) : si ce point de vigilance s'avère faux à
    l'usage (nom de paramètre différent), la 1re page reste toujours récupérée correctement,
    seule la pagination au-delà échouerait silencieusement (boucle simplement interrompue).

    Retourne une liste normalisée `{date, montant_signe, devise, libelle, source_id}` — même
    convention que `connector_powens.fetch_transactions`, jamais le JSON brut fournisseur
    transmis tel quel plus loin (Connector -> Adaptateur -> forme normalisée)."""
    credentials = json.loads(credentials_json)
    account_uid = compte_brick.get('contenu', {}).get('enablebanking_account_uid')
    if not account_uid:
        raise ValueError("Brique Compte sans enablebanking_account_uid — mapping à faire une fois via le flux d'autorisation Enable Banking")

    token = sign_jwt(credentials)
    headers = {'Authorization': f'Bearer {token}'}
    params = {'limit': page_limit}
    transactions_raw = []

    for _ in range(max_pages):
        r = requests.get(
            f'{EB_API_BASE}/accounts/{account_uid}/transactions',
            headers=headers, params=params, timeout=20,
        )
        r.raise_for_status()
        payload = r.json()
        page = payload.get('transactions', [])
        transactions_raw.extend(page)
        continuation_key = payload.get('continuation_key')
        if not continuation_key or not page:
            break
        params = {'limit': page_limit, 'continuation_key': continuation_key}

    normalized = []
    for t in transactions_raw:
        amount_info = t.get('transaction_amount') or {}
        montant = amount_info.get('amount')
        try:
            montant = float(montant) if montant is not None else None
        except (TypeError, ValueError):
            montant = None
        if montant is not None and t.get('credit_debit_indicator') == 'DBIT':
            montant = -abs(montant)
        elif montant is not None:
            montant = abs(montant)

        remittance = t.get('remittance_information') or []
        libelle = ' / '.join(str(x).strip() for x in remittance if x) if remittance else ''

        normalized.append({
            'date': t.get('booking_date') or t.get('value_date') or t.get('transaction_date'),
            'montant_signe': montant,
            'devise': amount_info.get('currency') or 'EUR',
            'libelle': libelle,
            'source_id': f"enablebanking_{t.get('entry_reference') or t.get('transaction_id') or t.get('booking_date')}_{montant}",
            '_raw': t,  # objet brut Enable Banking tel que reçu — consommé UNIQUEMENT par
                        # l'appelant (executor/app.py::_fetch_transactions_for_compte) pour le
                        # document source sanctuarisé (§2bis), jamais transmis tel quel plus loin.
        })

    return normalized
