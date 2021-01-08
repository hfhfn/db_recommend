namespace cpp profile_mp
namespace py profile_mp

struct KV
{
	1: i64 key,
	2: double weight,
}

struct User
{
	1: string uid,
	2: optional i32 u_age,
	3: optional i32 u_gender,
	4: optional i32 u_city,
	5: optional i32 u_register_time,
	6: optional i32 u_vip,
}

struct Content
{
	1: i64 itid,
	2: optional i32 i_duration,
	3: optional string i_category,
	4: optional double i_quality,
    5: optional i32 i_p_count,
    6: optional string i_title,
    7: optional string i_keyword,
    8: optional string i_actor,
    9: optional i32 i_year,
    10: optional i32 i_vip,
    11: optional string i_country,
    12: optional i64 i_author_id,
}

struct Author
{
    1: i64 aid,
}

struct CidAuthor
{
    1: i64 cid,
    2: Author author,
}

struct ContentProfileGetReq
{
    1: list<i64> cid_list,
}

struct ContentProfileGetRsp
{
    1: list<Content> content_list,
    2: string status,
}

struct UserProfileGetReq
{
    1: list<string> uid_list,
}

struct UserProfileGetRsp
{
    1: list<User> user_list,
    2: string status,
}

struct AuthorProfileAidGetReq
{
    1: list<i64> aid_list,
}

struct AuthorProfileAidGetRsp
{
    1: list<Author> author_list,
    2: string status,
}

struct AuthorProfileCidGetReq
{
    1: list<i64> cid_list,
}

struct AuthorProfileCidGetRsp
{
    1: list<CidAuthor> c_author_list,
	2: string status,
}

struct ContentProfilePutReq
{
    1: list<Content> content_list,
}

struct UserProfilePutReq
{
    1: list<User> user_list,
}

struct AuthorProfilePutReq
{
    1: list<Author> author_list,
}

struct ProfilePutRsp
{
    1: string status,
    2: i32 count,
}

service Profile
{
    // get
    ContentProfileGetRsp get_content_profiles_ex(1:ContentProfileGetReq req, 2:map<string,string> c),
    UserProfileGetRsp get_user_profiles_ex(1:UserProfileGetReq req, 2:map<string,string> c),
    AuthorProfileAidGetRsp get_author_aid_profiles_ex(1:AuthorProfileAidGetReq req, 2:map<string,string> c),

    // put
    ProfilePutRsp put_content_profiles(1:ContentProfilePutReq req, 2:map<string,string> c),
    ProfilePutRsp put_user_profiles(1:UserProfilePutReq req, 2:map<string,string> c),
    ProfilePutRsp put_author_profiles(1:AuthorProfilePutReq req, 2:map<string,string> c),
}




