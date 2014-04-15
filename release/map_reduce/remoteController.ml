open Async.Std
open Protocol

let ips = ref []

let init addrs =
  ips := addrs


module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =

    let worker_list = !ips in
    (*CHANGE THIS to execute in parallel and handle connection failures!*)
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
              | [] -> failwith "[Error] No workers for map!"
              | h'::t' -> (let (_,r,w) = h' in
                     (WReq.send w (WReq.MapRequest h) );
                     (h, h', (WRes.receive r)) :: (initial_mapping t (t' @ [h']))))
      in 
      let started_jobs = initial_mapping inputs conn_list in
      
    (*All of the jobs have been started, wait for them all to complete and restart any that fail*)

      let rec wait_for_completion running_jobs alive_workers = match running_jobs with
        | [] -> return ([],alive_workers)
        | h::t -> (let (job,conn,result) = h in
              result >>= fun res ->
                match res with
                 | `Eof -> ((print_endline "[Warning] Socket closed, map-worker presumed to be down; Restarting job");
                      let new_alive_workers = List.filter (fun p -> (p != conn)) alive_workers in
                      match new_alive_workers with
                        | [] -> failwith "[Error] All map-workers have died! Can't map"
                        | h'::t' -> (let (_,r,_) = h' in (wait_for_completion (t @ [(job,h',(WRes.receive r))]) (t' @ [h']))) 
                      )
                 | `Ok map_result -> (
                  match map_result with 
                    | WRes.JobFailed(s) -> failwith ("[Error]: Got 'JobFailed' from map-worker with message: " ^ s)
                      | WRes.MapResult(m) -> (wait_for_completion t alive_workers) >>= fun data -> return ((m :: fst(data)) ,snd(data))
                      | WRes.ReduceResult(_) -> failwith "[Error]: Got 'ReduceResult' from worker; Expected 'MapResult'"
                 ))
      in 
      let module C = Combiner.Make(Job) in
      (wait_for_completion started_jobs conn_list) >>= fun map_completion_result -> 
        let alive_workers = snd(map_completion_result) in
        let intermediate_data = C.combine( List.flatten ( fst(map_completion_result) )) in
         
        (*Begin reduce logic*)
        let rec initial_reducing input_list worker_list = match input_list with
        | [] -> []
        | h::t -> (match worker_list with
              | [] -> failwith "[Error] No workers for reduce!"
              | h'::t' -> (let (_,r,w) = h' in
                     (WReq.send w (WReq.ReduceRequest( fst h, snd h ) ) );
                     (h, h', (WRes.receive r)) :: (initial_reducing t (t' @ [h']))))
      in 
      let started_jobs = initial_reducing intermediate_data alive_workers in
      
    (*All of the jobs have been started, wait for them all to complete and restart any that fail*)

      let rec wait_for_completion running_jobs alive_workers = match running_jobs with
        | [] -> return []
        | h::t -> (let (job,conn,result) = h in
              result >>= fun res ->
                match res with
                 | `Eof -> ((print_endline "[Warning] Socket closed, reduce-worker presumed to be down; Restarting job");
                      let new_alive_workers = List.filter (fun p -> (p != conn)) alive_workers in
                      match new_alive_workers with
                        | [] -> failwith "[Error] All reduce-workers have died!"
                        | h'::t' -> (let (_,r,_) = h' in (wait_for_completion (t @ [(job,h',(WRes.receive r))]) (t' @ [h']))) 
                      )
                 | `Ok red_result -> (
                  match red_result with 
                    | WRes.JobFailed(s) -> failwith ("[Error]: Got 'JobFailed' from worker with message: " ^ s)
                      | WRes.MapResult(_) -> failwith "[Error]: Got 'MapResult' from worker; Expected 'ReduceResult'"
                      | WRes.ReduceResult(r) -> (wait_for_completion t alive_workers) >>= fun data -> return ((fst(job),r) :: data)
                 ))
      in 
      wait_for_completion started_jobs alive_workers
  
end

