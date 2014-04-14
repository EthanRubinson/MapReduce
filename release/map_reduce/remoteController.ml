open Async.Std
open Protocol

let ips = ref []

let init addrs =
  ips := addrs


module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =

    let worker_list = !ips in
    let rec connect_and_initialize_workers = function
      | [] -> return []
      | h::t -> (Tcp.connect (Tcp.to_host_and_port (fst h) (snd h) )) >>= fun connection ->
          let (_,_,writer) = connection in
          (Writer.write writer Job.name);
          connect_and_initialize_workers t >>= fun conn_list_tl ->
          return (connection :: conn_list_tl)
    in
    connect_and_initialize_workers worker_list >>= fun conn_list ->

    (*At this point we should have a list of connections to workers and they all have been sent
       the name of the job they are to be executing *)

      let module WReq = WorkerRequest(Job) in
      let module WRes = WorkerResponse(Job) in
      failwith "Not Implemented"

  (* Parse the input data and initialize a map job for the workers with the parsed input *)

  (* Wait for all the workers to finish, putting their data into a aQueue, and combine the intermediary data with the same keys *)

  (* Parse the aggregated intermediate data and initialize a reduce job for the workers with the parsed input*)

  (* Wait for all of the reduce workers to finish *)

  (* Combine the reduce results, and return to the user *)
  
end

