## Match all parents comment with not enough children
match (parent:Comment)--(child:Comment)
with parent,count(child) as children
where parent.totalReplyCount > children return parent.commentId,parent.totalReplyCount,children

## Get all videoIds for Minecraft
match (v:Video)--(g:Game{title:"Minecraft"}) 
return v.videoId

## same but only that has not comments:
match (v:Video)--(g:Game{title:"Minecraft"}) where not (v)--(:Comment) return v.videoId

## komentarze dreama
match (ch:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(c:Comment) return c

## parent comments for dreama

match (ch:Channel{channelId:"channel/UCA2tt9GSU2sl8rAqjlLR3mQ"})--(v:Video)--(parent:Comment)--(child:Comment)
with parent,count(child) as children
where parent.totalReplyCount > children return parent.commentId,parent.totalReplyCount,children

match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})<--(video:Video)<--(parent:Comment)--(child:Comment)
with parent,video,count(child) as children
where parent.totalReplyCount > children
return c.commentId,v.videoId