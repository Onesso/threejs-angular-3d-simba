import {
  Component,
  ElementRef,
  AfterViewInit,
  OnDestroy,
  ViewChild,
  signal,
} from '@angular/core';
import { SceneManager, ViewMode } from '../three/scene-manager';
import { RoomDef } from '../three/house-builder';

@Component({
  selector: 'app-scene',
  standalone: true,
  templateUrl: './scene.component.html',
  styleUrl: './scene.component.scss',
})
export class SceneComponent implements AfterViewInit, OnDestroy {
  @ViewChild('sceneContainer', { static: true }) containerRef!: ElementRef<HTMLDivElement>;
  @ViewChild('renderCanvas', { static: true }) canvasRef!: ElementRef<HTMLCanvasElement>;

  private sceneManager!: SceneManager;

  protected mode = signal<ViewMode>('orbit');
  protected labelsOn = signal(true);
  protected dimensionsOn = signal(false);
  protected selectedRoom = signal<RoomDef | null>(null);
  protected showHelp = signal(false);

  ngAfterViewInit(): void {
    this.sceneManager = new SceneManager(
      this.containerRef.nativeElement,
      this.canvasRef.nativeElement,
      {
        onRoomClick: (room) => this.selectedRoom.set(room),
        onModeChange: (m) => this.mode.set(m),
      }
    );
  }

  ngOnDestroy(): void {
    this.sceneManager?.dispose();
  }

  protected toggleMode(): void {
    const next: ViewMode = this.mode() === 'orbit' ? 'walkthrough' : 'orbit';
    this.sceneManager.setMode(next);
    this.mode.set(next);
  }

  protected toggleLabels(): void {
    const on = this.sceneManager.toggleLabels();
    this.labelsOn.set(on);
  }

  protected toggleDimensions(): void {
    const on = this.sceneManager.toggleDimensions();
    this.dimensionsOn.set(on);
  }

  protected closeRoomPanel(): void {
    this.selectedRoom.set(null);
  }

  protected toggleHelp(): void {
    this.showHelp.set(!this.showHelp());
  }

  protected getRoomColor(room: RoomDef): string {
    return '#' + room.color.toString(16).padStart(6, '0');
  }
}
