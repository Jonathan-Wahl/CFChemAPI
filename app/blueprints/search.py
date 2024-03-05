# blueprints/search.py

from flask import Blueprint, request
from database.database import database
search = Blueprint('search', __name__, url_prefix="/search")

@search.route('/<include_sources>')
@search.route('/')
def index(include_sources: bool = False):
    """Endpoint returning lincs objects based on search parameter (query)
    This endpoint allows you to return a list of Lincs objects.
    ---
    parameters:
      - name: query
        in: query
        type: string
        required: false
      - name: limit
        in: query
        type: int
        default: 10
        required: false
      - name: offset
        in: query
        type: int
        default: 0
        required: false
    responses:
      200:
        description: A list of Lincs objects
      400:
        description: Malformed request error
    """

    # Limit the query size
    rawInput = request.args.get('query', type=str) or ''
    limit = request.args.get('limit', type=int) or 10
    offset = request.args.get('offset', type=int) or 0
    # Build the query
    input = '%' + rawInput + '%'
    searchCollection = database.select("""
    SELECT * FROM lincs
    WHERE moa LIKE %(input)s
    OR cansmi LIKE %(input)s
    OR inchi_key LIKE %(input)s
    OR lcs_id LIKE %(input)s
    OR pert_name LIKE %(input)s
    OR smiles LIKE %(input)s
    OR target LIKE %(input)s
    LIMIT %(limit)s
    OFFSET %(offset)s
    """, {'input': input, 'rawinput': rawInput, 'limit': limit, 'offset': offset})
    if include_sources:
        SOURCES = ["idg", "drugcentral", "refmet", "lincs", "glygen", "reprotox"]
        for dict_row in searchCollection:
          mol_id = dict_row["mol_id"]
          mol_sources = database.get_sources(mol_id, SOURCES)
          # this assumes there is not a "sources" key in the dict_row
          dict_row["sources"] = mol_sources
    return searchCollection