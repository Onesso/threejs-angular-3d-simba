import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { PointerLockControls } from 'three/examples/jsm/controls/PointerLockControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
import {
  buildContainerHouse,
  ROOMS,
  DIMENSIONS,
  HOUSE_LENGTH,
  HOUSE_WIDTH,
  HOUSE_HEIGHT,
  type RoomDef,
} from './house-builder';

export type ViewMode = 'orbit' | 'walkthrough';

export interface SceneManagerCallbacks {
  onRoomClick?: (room: RoomDef | null) => void;
  onModeChange?: (mode: ViewMode) => void;
}

export class SceneManager {
  private renderer!: THREE.WebGLRenderer;
  private labelRenderer!: CSS2DRenderer;
  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private orbitControls!: OrbitControls;
  private pointerControls!: PointerLockControls;
  private house!: THREE.Group;
  private raycaster = new THREE.Raycaster();
  private mouse = new THREE.Vector2();
  private animationId = 0;
  private clock = new THREE.Clock();

  // Walkthrough state
  private moveForward = false;
  private moveBackward = false;
  private moveLeft = false;
  private moveRight = false;
  private velocity = new THREE.Vector3();
  private direction = new THREE.Vector3();
  private moveSpeed = 5.0;

  // Labels
  private roomLabels: CSS2DObject[] = [];
  private dimensionLabels: CSS2DObject[] = [];
  private dimensionLines: THREE.Line[] = [];
  private labelsVisible = true;
  private dimensionsVisible = false;

  // Door animation
  private doorIsOpen = true;
  private doorTarget = 1; // 1 = open, 0 = closed
  private doorProgress = 1; // current animation progress (0-1)

  // Window animation
  private windowsOpen = false;
  private windowTarget = 0; // 0 = closed, 1 = open
  private windowProgress = 0;

  private mode: ViewMode = 'orbit';
  private callbacks: SceneManagerCallbacks;
  private container: HTMLElement;

  constructor(container: HTMLElement, canvas: HTMLCanvasElement, callbacks: SceneManagerCallbacks = {}) {
    this.container = container;
    this.callbacks = callbacks;
    this.initRenderer(canvas);
    this.initScene();
    this.initCamera();
    this.initControls();
    this.initLighting();
    this.initLabelRenderer();
    this.buildHouse();
    this.createLabels();
    this.createDimensionMarkers();
    this.bindEvents();
    this.animate();
  }

  private initRenderer(canvas: HTMLCanvasElement): void {
    this.renderer = new THREE.WebGLRenderer({
      canvas,
      antialias: true,
      alpha: false,
    });
    this.renderer.setSize(this.container.clientWidth, this.container.clientHeight);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.shadowMap.enabled = true;
    this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    this.renderer.toneMappingExposure = 1.2;
  }

  private initScene(): void {
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0xc8e6ff); // Light sky blue
    this.scene.fog = new THREE.Fog(0xc8e6ff, 30, 80);
  }

  private initCamera(): void {
    const aspect = this.container.clientWidth / this.container.clientHeight;
    this.camera = new THREE.PerspectiveCamera(55, aspect, 0.1, 200);
    this.camera.position.set(12, 8, -14);
    this.camera.lookAt(0, 1, 0);
  }

  private initControls(): void {
    // Orbit controls (default)
    this.orbitControls = new OrbitControls(this.camera, this.renderer.domElement);
    this.orbitControls.target.set(0, 1, 0);
    this.orbitControls.enableDamping = true;
    this.orbitControls.dampingFactor = 0.08;
    this.orbitControls.maxPolarAngle = Math.PI / 2.05;
    this.orbitControls.minDistance = 3;
    this.orbitControls.maxDistance = 40;
    this.orbitControls.update();

    // Pointer lock controls (walkthrough)
    this.pointerControls = new PointerLockControls(this.camera, this.renderer.domElement);
    this.pointerControls.addEventListener('lock', () => {
      this.callbacks.onModeChange?.('walkthrough');
    });
    this.pointerControls.addEventListener('unlock', () => {
      // Stay in walkthrough mode but show cursor
    });
  }

  private initLighting(): void {
    // Hemisphere light (sky + ground bounce)
    const hemiLight = new THREE.HemisphereLight(0x87ceeb, 0x8b7355, 0.6);
    this.scene.add(hemiLight);

    // Ambient for fill
    const ambient = new THREE.AmbientLight(0xffffff, 0.3);
    this.scene.add(ambient);

    // Main directional light (sun)
    const sun = new THREE.DirectionalLight(0xfff5e0, 1.5);
    sun.position.set(15, 20, -10);
    sun.castShadow = true;
    sun.shadow.mapSize.width = 2048;
    sun.shadow.mapSize.height = 2048;
    sun.shadow.camera.left = -20;
    sun.shadow.camera.right = 20;
    sun.shadow.camera.top = 20;
    sun.shadow.camera.bottom = -20;
    sun.shadow.camera.near = 0.5;
    sun.shadow.camera.far = 60;
    sun.shadow.bias = -0.001;
    this.scene.add(sun);

    // Subtle fill light from opposite side
    const fill = new THREE.DirectionalLight(0xaaccff, 0.4);
    fill.position.set(-10, 8, 10);
    this.scene.add(fill);
  }

  private initLabelRenderer(): void {
    this.labelRenderer = new CSS2DRenderer();
    this.labelRenderer.setSize(this.container.clientWidth, this.container.clientHeight);
    this.labelRenderer.domElement.style.position = 'absolute';
    this.labelRenderer.domElement.style.top = '0';
    this.labelRenderer.domElement.style.left = '0';
    this.labelRenderer.domElement.style.pointerEvents = 'none';
    this.container.appendChild(this.labelRenderer.domElement);
  }

  private buildHouse(): void {
    this.house = buildContainerHouse();
    this.scene.add(this.house);
  }

  private createLabels(): void {
    for (const room of ROOMS) {
      const div = document.createElement('div');
      div.className = 'room-label';
      div.textContent = room.name;
      div.style.cssText = `
        background: rgba(0,0,0,0.7);
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-family: system-ui, sans-serif;
        white-space: nowrap;
        border-left: 3px solid #${room.color.toString(16).padStart(6, '0')};
      `;

      const label = new CSS2DObject(div);
      // Adjust for house offset
      label.position.copy(room.labelPos);
      label.position.x -= HOUSE_LENGTH / 2;
      label.position.z -= HOUSE_WIDTH / 2;
      this.roomLabels.push(label);
      this.scene.add(label);
    }
  }

  private createDimensionMarkers(): void {
    const lineMat = new THREE.LineBasicMaterial({ color: 0xff4444, linewidth: 2 });

    for (const dim of DIMENSIONS) {
      // Offset for house position
      const s = dim.start.clone().add(new THREE.Vector3(-HOUSE_LENGTH / 2, 0, -HOUSE_WIDTH / 2));
      const e = dim.end.clone().add(new THREE.Vector3(-HOUSE_LENGTH / 2, 0, -HOUSE_WIDTH / 2));

      // Line
      const points = [s, e];
      const lineGeo = new THREE.BufferGeometry().setFromPoints(points);
      const dimLineMat = dim.color
        ? new THREE.LineBasicMaterial({ color: dim.color })
        : lineMat;
      const line = new THREE.Line(lineGeo, dimLineMat);
      line.visible = this.dimensionsVisible;
      this.dimensionLines.push(line);
      this.scene.add(line);

      // Label
      const mid = s.clone().add(e).multiplyScalar(0.5).add(dim.offset);
      const div = document.createElement('div');
      div.className = 'dim-label';
      div.textContent = dim.label;
      const bgColor = dim.color || 'rgba(255,60,60,0.85)';
      div.style.cssText = `
        background: ${bgColor};
        color: white;
        padding: 2px 8px;
        border-radius: 3px;
        font-size: 11px;
        font-weight: bold;
        font-family: monospace;
      `;

      const label = new CSS2DObject(div);
      label.position.copy(mid);
      label.visible = this.dimensionsVisible;
      this.dimensionLabels.push(label);
      this.scene.add(label);
    }
  }

  private bindEvents(): void {
    window.addEventListener('resize', this.onResize);
    this.renderer.domElement.addEventListener('click', this.onClick);
    document.addEventListener('keydown', this.onKeyDown);
    document.addEventListener('keyup', this.onKeyUp);
  }

  private onResize = (): void => {
    const w = this.container.clientWidth;
    const h = this.container.clientHeight;
    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(w, h);
    this.labelRenderer.setSize(w, h);
  };

  private onClick = (event: MouseEvent): void => {
    const rect = this.renderer.domElement.getBoundingClientRect();
    this.mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    this.mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    this.raycaster.setFromCamera(this.mouse, this.camera);
    const intersects = this.raycaster.intersectObjects(this.house.children, true);

    let foundRoom: RoomDef | null = null;
    for (const hit of intersects) {
      if (hit.object.userData?.['room']) {
        foundRoom = hit.object.userData['room'] as RoomDef;
        break;
      }
    }

    this.callbacks.onRoomClick?.(foundRoom);
  };

  private onKeyDown = (event: KeyboardEvent): void => {
    if (this.mode !== 'walkthrough') return;
    switch (event.code) {
      case 'KeyW':
      case 'ArrowUp':
        this.moveForward = true;
        break;
      case 'KeyS':
      case 'ArrowDown':
        this.moveBackward = true;
        break;
      case 'KeyA':
      case 'ArrowLeft':
        this.moveLeft = true;
        break;
      case 'KeyD':
      case 'ArrowRight':
        this.moveRight = true;
        break;
    }
  };

  private onKeyUp = (event: KeyboardEvent): void => {
    switch (event.code) {
      case 'KeyW':
      case 'ArrowUp':
        this.moveForward = false;
        break;
      case 'KeyS':
      case 'ArrowDown':
        this.moveBackward = false;
        break;
      case 'KeyA':
      case 'ArrowLeft':
        this.moveLeft = false;
        break;
      case 'KeyD':
      case 'ArrowRight':
        this.moveRight = false;
        break;
    }
  };

  private updateWalkthrough(delta: number): void {
    if (this.mode !== 'walkthrough') return;

    this.velocity.x -= this.velocity.x * 10.0 * delta;
    this.velocity.z -= this.velocity.z * 10.0 * delta;

    this.direction.z = Number(this.moveForward) - Number(this.moveBackward);
    this.direction.x = Number(this.moveRight) - Number(this.moveLeft);
    this.direction.normalize();

    if (this.moveForward || this.moveBackward) {
      this.velocity.z -= this.direction.z * this.moveSpeed * delta;
    }
    if (this.moveLeft || this.moveRight) {
      this.velocity.x -= this.direction.x * this.moveSpeed * delta;
    }

    this.pointerControls.moveRight(-this.velocity.x * delta);
    this.pointerControls.moveForward(-this.velocity.z * delta);

    // Keep at eye height
    this.camera.position.y = 1.6;
  }

  private animate = (): void => {
    this.animationId = requestAnimationFrame(this.animate);
    const delta = this.clock.getDelta();

    if (this.mode === 'orbit') {
      this.orbitControls.update();
    } else {
      this.updateWalkthrough(delta);
    }

    // Animate door open/close
    this.updateDoorAnimation(delta);
    // Animate windows open/close
    this.updateWindowAnimation(delta);

    this.renderer.render(this.scene, this.camera);
    this.labelRenderer.render(this.scene, this.camera);
  };

  private updateDoorAnimation(delta: number): void {
    if (!this.house) return;
    const diff = this.doorTarget - this.doorProgress;
    if (Math.abs(diff) < 0.001) return;

    const speed = 2.0;
    this.doorProgress += Math.sign(diff) * speed * delta;
    this.doorProgress = Math.max(0, Math.min(1, this.doorProgress));

    try {
      const leftLeaf = this.house.userData['doorLeftLeaf'] as THREE.Object3D | undefined;
      const rightLeaf = this.house.userData['doorRightLeaf'] as THREE.Object3D | undefined;
      const openAngle = (this.house.userData['doorOpenAngle'] as number) || Math.PI * 0.4;

      if (leftLeaf && rightLeaf) {
        const angle = openAngle * this.doorProgress;
        leftLeaf.rotation.y = angle;
        rightLeaf.rotation.y = -angle; // mirrored via scale.x = -1
      }
    } catch {
      // silently handle if userData not ready
    }
  }

  private updateWindowAnimation(delta: number): void {
    if (!this.house) return;
    const diff = this.windowTarget - this.windowProgress;
    if (Math.abs(diff) < 0.001) return;

    const speed = 2.0;
    this.windowProgress += Math.sign(diff) * speed * delta;
    this.windowProgress = Math.max(0, Math.min(1, this.windowProgress));

    try {
      const leaves = this.house.userData['windowLeaves'] as { leaf: THREE.Object3D; sign: number }[] | undefined;
      const openAngle = (this.house.userData['windowOpenAngle'] as number) || Math.PI * 0.35;

      if (leaves) {
        for (const { leaf, sign } of leaves) {
          leaf.rotation.y = sign * openAngle * this.windowProgress;
        }
      }
    } catch {
      // silently handle if userData not ready
    }
  }

  // ---- Public API ----

  setMode(mode: ViewMode): void {
    this.mode = mode;

    if (mode === 'orbit') {
      this.pointerControls.unlock();
      this.orbitControls.enabled = true;
      this.camera.position.set(12, 8, -14);
      this.orbitControls.target.set(0, 1, 0);
      this.orbitControls.update();
    } else {
      this.orbitControls.enabled = false;
      // Position camera inside the living room
      this.camera.position.set(-HOUSE_LENGTH / 2 + 2.5, 1.6, 0);
      this.camera.lookAt(-HOUSE_LENGTH / 2 + 6, 1.6, 0);
      this.pointerControls.lock();
    }

    this.callbacks.onModeChange?.(mode);
  }

  getMode(): ViewMode {
    return this.mode;
  }

  toggleLabels(): boolean {
    this.labelsVisible = !this.labelsVisible;
    for (const label of this.roomLabels) {
      label.visible = this.labelsVisible;
    }
    return this.labelsVisible;
  }

  toggleDoor(): boolean {
    this.doorIsOpen = !this.doorIsOpen;
    this.doorTarget = this.doorIsOpen ? 1 : 0;
    return this.doorIsOpen;
  }

  getDoorOpen(): boolean {
    return this.doorIsOpen;
  }

  toggleWindows(): boolean {
    this.windowsOpen = !this.windowsOpen;
    this.windowTarget = this.windowsOpen ? 1 : 0;
    return this.windowsOpen;
  }

  getWindowsOpen(): boolean {
    return this.windowsOpen;
  }

  toggleDimensions(): boolean {
    this.dimensionsVisible = !this.dimensionsVisible;
    for (const label of this.dimensionLabels) {
      label.visible = this.dimensionsVisible;
    }
    for (const line of this.dimensionLines) {
      line.visible = this.dimensionsVisible;
    }
    return this.dimensionsVisible;
  }

  dispose(): void {
    cancelAnimationFrame(this.animationId);
    window.removeEventListener('resize', this.onResize);
    this.renderer.domElement.removeEventListener('click', this.onClick);
    document.removeEventListener('keydown', this.onKeyDown);
    document.removeEventListener('keyup', this.onKeyUp);
    this.orbitControls.dispose();
    this.pointerControls.dispose();
    this.renderer.dispose();
    this.labelRenderer.domElement.remove();
  }
}
