from orbit.hitl import chosen_option, responder

APPROVED = {
    "chosen_options": ["Approve"],
    "params_input": {},
    "responded_by_user": {"id": "admin", "name": "admin"},
}


def test_the_single_choice_comes_out_of_the_list():
    assert chosen_option(APPROVED, "Reject") == "Approve"


def test_no_answer_falls_back():
    assert chosen_option({"chosen_options": []}, "Reject") == "Reject"
    assert chosen_option({}, "Reject") == "Reject"
    assert chosen_option(None, "Reject") == "Reject"


def test_the_responder_is_a_name_not_a_user_object():
    """responded_by_user is a dict. Handing it straight to SQLite raises
    "type 'dict' is not supported" and fails the task after the human already
    said yes."""
    assert responder(APPROVED) == "admin"


def test_the_responder_falls_back_to_id_when_unnamed():
    assert responder({"responded_by_user": {"id": "u-7"}}) == "u-7"


def test_a_plain_string_responder_is_kept():
    assert responder({"responded_by_user": "alice"}) == "alice"


def test_an_unanswered_card_is_attributed_to_the_timeout():
    assert responder({}) == "timeout"
    assert responder({"responded_by_user": None}) == "timeout"
    assert responder(None) == "timeout"
