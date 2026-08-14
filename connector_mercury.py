"""Connector Mercury — fetch(compte_brick) -> [{solde, devise?, date?}, ...] (§8
ARCHITECTURE.md Suivre Mes Comptes). Isolé : si Mercury change son API, seul ce fichier
change, rien d'autre dans l'Executor.

API Mercury (docs.mercury.com) : base https://api.mercury.com/api/v1/, auth Bearer avec un
token qui contient déjà son propre préfixe littéral "secret-token:" (pas un schéma OAuth —
le header est bien "Bearer secret-token:<valeur>", vérifié dans la doc officielle). Un seul
appel GET /accounts renvoie TOUS les comptes Mercury du token (Checking + Savings compris),
d'où le filtrage par correspondance de nom ci-dessous.
"""
import requests

MERCURY_API_BASE = 'https://api.mercury.com/api/v1'


def fetch(compte_brick, api_key):
    """compte_brick.contenu.nom doit correspondre (insensible à la casse, sous-chaîne) au
    `name`/`nickname` retourné par Mercury pour l'un de ses comptes — pas de mapping par id
    stocké en dur, résolu à chaque appel (cohérent avec le principe "jamais de lien
    compte->connector stocké en dur", ARCHITECTURE.md §2)."""
    contenu = compte_brick.get('contenu', {})
    nom = (contenu.get('nom') or '').lower()
    # "Mercury" (l'établissement) apparaît dans TOUS les labels retournés par l'API — l'exclure
    # du matching, sinon le 1er compte trouvé "gagne" par ce mot commun au lieu du bon compte
    # (bug réel trouvé en testant : "Mercury Savings" matchait "Mercury Checking" à cause du
    # mot "mercury" seul).
    etablissement = (contenu.get('etablissement') or '').lower()
    nom_mots_distinctifs = [w for w in nom.split() if len(w) > 3 and w != etablissement]

    # Le token Mercury est parfois copié avec son préfixe "secret-token:" déjà inclus, parfois
    # sans — gérer les deux plutôt que de le doubler par erreur (bug évité en préparant
    # l'enregistrement de la vraie clé de Stéphane, 2026-07-25).
    token = api_key if api_key.startswith('secret-token:') else f'secret-token:{api_key}'
    r = requests.get(
        f'{MERCURY_API_BASE}/accounts',
        headers={'Authorization': f'Bearer {token}'},
        timeout=15,
    )
    r.raise_for_status()
    accounts = r.json().get('accounts', [])

    match = None
    for acc in accounts:
        label = f"{acc.get('name', '')} {acc.get('nickname') or ''}".lower()
        if label and (label in nom or nom in label or any(word in label for word in nom_mots_distinctifs)):
            match = acc
            break

    if not match:
        raise ValueError(f"Aucun compte Mercury ne correspond à '{contenu.get('nom')}' parmi {len(accounts)} comptes renvoyés par l'API")

    return [{'solde': match['currentBalance'], 'devise': 'USD'}]
