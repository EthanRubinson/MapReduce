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
          (Writer.write_line writer Job.name);
          connect_and_initialize_workers t >>= fun conn_list_tl ->
          return (connection :: conn_list_tl)
    in
    connect_and_initialize_workers worker_list >>= fun conn_list ->

    (*At this point we should have a list of connections to workers and they all have been sent
       the name of the job they are to be executing *)

      let module WReq = WorkerRequest(Job) in
      let module WRes = WorkerResponse(Job) in

      let rec initial_mapping input_list worker_list = match input_list with
        | [] -> []
        | h::t -> (match worker_list with
              | [] -> failwith "[Error] No workers!"
              | h'::t' -> (let (_,r,w) = h' in
                     (WReq.send w (WReq.MapRequest h) );
                     (h, h', (WRes.receive r)) :: (initial_mapping t (t' @ [h']))))
      in 
      let started_jobs = initial_mapping inputs conn_list in
      
      let rec wait_for_completion running_jobs alive_workers = match running_jobs with
        | [] -> return []
        | h::t -> (let (job,conn,result) = h in
              result >>= fun res ->
                match res with
                 | `Eof -> ((print_endline "[Warning] Socket closed, worker presumed to be down; Restarting job");
                      let new_alive_workers = List.filter (fun p -> (p != conn)) alive_workers in
                      match new_alive_workers with
                        | [] -> failwith "[Error] All workers have died!"
                        | h'::t' -> (let (_,r,_) = h' in (wait_for_completion (t @ [(job,h',(WRes.receive r))]) (t' @ [h']))) 
                      )
                 | `Ok map_result -> (
                  match map_result with 
                    | WRes.JobFailed(s) -> failwith ("[Error]: Got 'JobFailed' from worker with message: " ^ s)
                      | WRes.MapResult(m) -> (wait_for_completion t alive_workers) >>= fun data -> return (m :: data)
                      | WRes.ReduceResult(_) -> failwith "[Error]: Got 'ReduceResult' from worker; Expected 'MapResult'"
                 ))
      in 
      ignore(wait_for_completion started_jobs conn_list);

      failwith "Not Implemented"

  (* Parse the input data and initialize a map job for the workers with the parsed input *)

  (* Wait for all the workers to finish, putting their data into a aQueue, and combine the intermediary data with the same keys *)

  (* Parse the aggregated intermediate data and initialize a reduce job for the workers with the parsed input*)

  (* Wait for all of the reduce workers to finish *)

  (* Combine the reduce results, and return to the user *)
  
end

