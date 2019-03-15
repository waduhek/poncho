$(document).ready(function(){


$('#submitmsg').click(function(){

//console.log('clicked')
var input = $('#usermsg').val();
console.log(input);

$('#chatbox').append('<p style="font-size:20px"><span>You: </span>'+input+'</p>');
$('#usermsg').val("");
$.post("/msg",{

	data:input
},function(data){

console.log(data);
$('#chatbox').append('<p style="font-size:20px"><span>Poncho: </span>'+data+'</p>');

});


});

});