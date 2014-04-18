open Async.Std
open Protocol
open AQueue
open Warmup

let ips = ref []

let init addrs =
  ips := addrs

module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =

    let print_worker = fun ip port -> ip ^ ":" ^ string_of_int port in

    let worker_list = !ips in
    let alive_queue = create () in
    let num_alive_workers = ref 0 in
    let connect_and_initialize_workers = fun (ip,port) ->
      (print_endline ("[INFO] Attempting to connect to worker at " ^ print_worker ip port));
      try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port)) )
      >>= function
        | Core.Std.Result.Error e -> (print_endline ("[ERROR] Failed to connect to worker at " ^ print_worker ip port));
                       return ()
        | Core.Std.Result.Ok  v -> ((print_endline ("[INFO] Connected to worker at " ^ print_worker ip port));
                       let (_,_,w) = v in 
                         try_with (fun () -> return (Writer.write_line w Job.name))
                         >>= function
                             | Core.Std.Result.Error e -> (print_endline ("[ERROR] Failed to initialize worker at " ^ print_worker ip port));
                                            return ()
                             | Core.Std.Result.Ok    v' -> (print_endline ("[INFO] Initialized worker at " ^ print_worker ip port));
                                             (num_alive_workers := !num_alive_workers + 1);
                                             return (push alive_queue v) )
    in
    deferred_map worker_list connect_and_initialize_workers 
    >>= fun _ -> 
    
      if !num_alive_workers == 0 then
        failwith "[FATAL] InfrastructureFailure; Could not connect to any workers"
      else

      let module WReq = WorkerRequest(Job) in
      let module WRes = WorkerResponse(Job) in

      let rec mapperFunc inputElement = pop alive_queue 
      >>= fun worker -> 
        match worker with
          | (s,r,w) -> (WReq.send w (WReq.MapRequest inputElement));
                 WRes.receive r
                 >>= fun res -> match res with
                  |`Eof -> (print_endline "[ERROR] A worker returned `Eof; Closing socket and reassigning work");
                       (Socket.shutdown s `Both);
                       (num_alive_workers := !num_alive_workers - 1);
                       if (!num_alive_workers <= 0) then
                        failwith "[FATAL] InfrastructureFailure; All workers are dead"
                       else
                        mapperFunc inputElement

                  |`Ok msg -> match msg with 
                    | WRes.JobFailed(err_msg) -> failwith ("[FATAL] MapFailed; " ^ err_msg)
                      | WRes.MapResult(map_res) ->
                                       (push alive_queue worker);
                                       return map_res
                      | _ -> (print_endline "[ERROR] A worker returned an innapropriate response, closing socket and reassigning work");
                       (Socket.shutdown s `Both);
                       (num_alive_workers := !num_alive_workers - 1);
                       if (!num_alive_workers <= 0) then
                        failwith "[FATAL] InfrastructureFailure; All workers are dead"
                       else
                        mapperFunc inputElement
      in
      deferred_map inputs mapperFunc 
      >>= fun map_lst -> 
        let module C = Combiner.Make(Job) in
        let intermediate_data = C.combine( List.flatten map_lst ) in

        let rec reducerFunc inputElement = pop alive_queue
        >>= fun worker ->
          match worker with
            |(s,r,w) -> (WReq.send w (WReq.ReduceRequest (fst inputElement, snd inputElement)));
                  WRes.receive r
                  >>= fun res -> match res with
                     |`Eof -> (print_endline "[ERROR] A worker returned `Eof; Closing socket and reassigning work");
                          (Socket.shutdown s `Both);
                          (num_alive_workers := !num_alive_workers - 1);
                          if (!num_alive_workers <= 0) then
                          failwith "[FATAL] InfrastructureFailure; All workers are dead"
                        else
                          reducerFunc inputElement
                     |`Ok msg -> match msg with 
                        | WRes.JobFailed(err_msg) -> failwith ("[FATAL] ReduceFailed; " ^ err_msg)
                          | WRes.ReduceResult(red_res) ->
                                       (push alive_queue worker);
                                       return (fst(inputElement), red_res)
                          | _ -> (print_endline "[ERROR] A worker returned an innapropriate response, closing socket and reassigning work");
                           (Socket.shutdown s `Both);
                           (num_alive_workers := !num_alive_workers - 1);
                           if (!num_alive_workers <= 0) then
                            failwith "[FATAL] InfrastructureFailure; All workers are dead"
                           else
                            reducerFunc inputElement
        in
      deferred_map intermediate_data reducerFunc
      >>= fun red_lst -> return red_lst
end
