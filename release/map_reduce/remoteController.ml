open Async.Std
open Async_extra

let ips = ref []

let init addrs =
  ips := addrs


module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =

  	let worker_list = !ips in
  	let rec connect_to_workers = function
  		| [] -> return []
  		| h::t -> (Tcp.connect (Tcp.to_host_and_port (fst h) (snd h) )) >>= fun worker ->  
  				connect_to_workers t >>= fun rest_of_workers ->
  				return (worker :: rest_of_workers);
  	in
  	let connected_workers = connect_to_workers worker_list in

  	let workerreq = WorkerRequest(Job) in 
  	let workerres = WorkerResponse(Job) in

  	Job.name

  	(*At this point we should have a list of workers we are connected
  	  to in the variable connected_workers*)

	(*Create a job based on the input*)

	(*Send a message to all of the workers with the name of the job to complete*)

	(* Parse the input data and initialize a map job for the workers with the parsed input *)

	(* Wait for all the workers to finish, putting their data into a aQueue, and combine the intermediary data with the same keys *)

	(* Parse the aggregated intermediate data and initialize a reduce job for the workers with the parsed input*)

	(* Wait for all of the reduce workers to finish *)

	(* Combine the reduce results, and return to the user *)


    failwith "Nowhere special."

end

