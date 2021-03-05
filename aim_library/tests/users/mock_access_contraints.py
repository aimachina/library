MOCK_USERS = {
    "user_one@sample.com": {"branch": "Sample Branch 1",},
    "user_two@sample.com": {"branch": "Sample Branch 1",},
    "user_three@sample.com": {"branch": "Sample Branch 2",},
    "root": {"superuser": True},
}
is_superuser = lambda u: "superuser" in u and u["superuser"] == True


def mock_owners_of_accesssable_resources(user_details):
    branch = user_details["branch"]
    users = [
        user for user, details in MOCK_USERS.items() if "branch" in details and details["branch"] == branch
    ]
    return users


def mock_get_user_access_constraint(user_id):
    user_details = MOCK_USERS[user_id]
    if is_superuser(user_details):
        return {}
    owners_of_accessable_resources = mock_owners_of_accesssable_resources(user_details)
    constraint = {"user_id": {"$in": owners_of_accessable_resources}}
    return constraint
